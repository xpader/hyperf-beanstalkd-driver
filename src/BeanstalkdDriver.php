<?php
/**
 * Created by PhpStorm.
 * User: pader
 * Date: 2019/12/10
 * Time: 15:05
 */

namespace pader\HyperfBeanstalkdDriver;

use Hyperf\AsyncQueue\Event\AfterHandle;
use Hyperf\AsyncQueue\Event\BeforeHandle;
use Hyperf\AsyncQueue\Event\FailedHandle;
use Hyperf\AsyncQueue\Event\RetryHandle;
use Hyperf\AsyncQueue\Exception\InvalidQueueException;
use Hyperf\AsyncQueue\JobInterface;
use Hyperf\AsyncQueue\Message;
use Hyperf\AsyncQueue\MessageInterface;
use pader\swbeanstalk\Swbeanstalk;
use Psr\Container\ContainerInterface;

class BeanstalkdDriver extends \Hyperf\AsyncQueue\Driver\Driver
{

	/**
	 * @var Swbeanstalk
	 */
	protected $client;

	/**
	 * @var string
	 */
	protected $tube;

	/**
	 * Max polling time.
	 * @var int
	 */
	protected $timeout;

	/**
	 * Retry delay time.
	 * @var array|int
	 */
	protected $retrySeconds;

	/**
	 * Handle timeout.
	 * @var int
	 */
	protected $handleTimeout;

	public function __construct(ContainerInterface $container, $config)
	{
		parent::__construct($container, $config);

		$client = new Swbeanstalk(
			env('BEANSTALKD_HOST', '127.0.0.1'),
			env('BEANSTALKD_PORT', 11300),
			env('BEANSTALKD_CONNECT_TIMEOUT', 1),
			env('BEANSTALKD_TIMEOUT', -1)
		);

		$this->tube = $config['tube'] ?? 'hyperf';
		$this->client = $client;

		$this->retrySeconds = $config['retry_seconds'] ?? 10;
		$this->handleTimeout = $config['handle_timeout'] ?? 10;
		$this->timeout = $config['timeout'];
	}

	private function connect()
	{
		if (!$this->client->isConnected()) {
			$this->client->connect();

			if ($this->tube != 'default') {
				$this->client->watch($this->tube);
				$this->client->ignore('default');
				$this->client->useTube($this->tube);
			}
		}
	}

	public function push(JobInterface $job, int $delay = 0): bool
	{
		$this->connect();

		$message = new Message($job);
		$data = $this->packer->pack($message);

		return (bool)$this->client->put($data, Swbeanstalk::DEFAULT_PRI, $delay, $this->handleTimeout);
	}

	public function delete(JobInterface $job): bool
	{
		//Unused method
		return true;
	}

	public function pop(): array
	{
		$this->connect();

		$job = $this->client->reserve($this->timeout);

		if (!$job) {
			return [false, null];
		}

		$message = $this->packer->unpack($job['body']);

		return [$job['id'], $message];
	}

	/**
	 * 确认消息已经完成
	 *
	 * @param $id
	 * @return bool
	 */
	public function ack($id): bool {
		$this->connect();
		return $this->client->delete($id);
	}

	/**
	 * 将消息置于失败状态
	 *
	 * @param $id
	 * @return bool
	 */
	public function fail($id): bool {
		$this->connect();
		return $this->client->bury($id);
	}

	public function flush(string $queue = null): bool {
		$this->connect();

		!$queue && $queue = 'failed';
		$peekMap = [
			'waiting' => 'peekReady',
			'delayed' => 'peekDelayed',
			'failed' => 'peekBuried'
		];
		$method = $peekMap[$queue] ?? null;

		if (!$method) {
			throw new InvalidQueueException(sprintf('Queue %s is not supported.', $queue));
		}

		while ($job = $this->client->{$method}()) {
			if (!$this->client->delete($job['id'])) {
				return false;
			}
		}

		return true;
	}

	public function reload(string $queue = null): int {
		$this->connect();

		if (!$queue) {
			$stats = $this->client->statsTube($this->tube);
			$num = $stats['current-jobs-bureid'] + $stats['current-jobs-delayed'];

			if ($num > 0) {
				$this->client->kick($num);
			}

			return $num;

		} else {
			$peekMap = [
				'delayed' => 'peekDelayed',
				'failed' => 'peekBuried'
			];

			$method = $peekMap[$queue] ?? null;

			if (!$method) {
				throw new InvalidQueueException(sprintf('Queue %s is not supported.', $queue));
			}

			$num = 0;

			while ($job = $this->client->{$method}()) {
				if ($this->client->peek($job['id'])) {
					++$num;
				}
			}

			return $num;
		}
	}

	public function info(): array {
		$this->connect();
		$stats = $this->client->statsTube($this->tube);
		return [
			'waiting' => $stats['current-jobs-ready'],
			'delayed' => $stats['current-jobs-delayed'],
			'failed' => $stats['current-jobs-buried'],
			'timeout' => 0,
			'reserved' => $stats['current-jobs-reserved']
		];
	}

	protected function retry(MessageInterface $message): bool {
		throw new \RuntimeException('No need to call retry when use BeanstalkdDriver.');
	}

	/**
	 * @param $id
	 * @param MessageInterface $message
	 * @return bool
	 */
	protected function release($id, $message) {
		$this->connect();
		$delay = time() + $this->getRetrySeconds($message->getAttempts());
		return $this->client->release($id, Swbeanstalk::DEFAULT_PRI, $delay);
	}

	protected function getRetrySeconds(int $attempts): int
	{
		if (! is_array($this->retrySeconds)) {
			return $this->retrySeconds;
		}

		if (empty($this->retrySeconds)) {
			return 10;
		}

		return $this->retrySeconds[$attempts - 1] ?? end($this->retrySeconds);
	}

	/**
	 * No need to call this.
	 * @inheritdoc
	 */
	protected function remove($id): bool {
		$this->connect();
		return $this->client->delete($id);
	}

	protected function getCallback($data, $message): callable
	{
		return function () use ($data, $message) {
			try {
				if ($message instanceof MessageInterface) {
					$this->event && $this->event->dispatch(new BeforeHandle($message));
					$message->job()->handle();
					$this->event && $this->event->dispatch(new AfterHandle($message));
				}

				$this->ack($data);
			} catch (\Throwable $ex) {
				if (isset($message, $data)) {
					if ($message->attempts()) {
						$this->event && $this->event->dispatch(new RetryHandle($message, $ex));
						$this->release($data, $message);
					} else {
						$this->event && $this->event->dispatch(new FailedHandle($message, $ex));
						$this->fail($data);
					}
				}
			}
		};
	}

}
