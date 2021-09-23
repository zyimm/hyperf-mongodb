<?php


namespace Hyperf\Mongodb\Pool;

use Hyperf\Di\Container;
use Hyperf\Di\Exception\NotFoundException;
use Psr\Container\ContainerInterface;
use Swoole\Coroutine\Channel;

class PoolFactory
{
    /**
     * @var ContainerInterface
     */
    protected $container;

    /**
     * @var Channel[]
     */
    protected $pools = [];

    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;
    }

    /**
     * @throws NotFoundException
     */
    public function getPool(string $name): Channel
    {
        if (isset($this->pools[$name])) {
            return $this->pools[$name];
        }

        if ($this->container instanceof Container) {
            $pool = $this->container->make(MongoDBPool::class, ['name' => $name]);
        } else {
            $pool = new MongoDBPool($this->container, $name);
        }
        return $this->pools[$name] = $pool;
    }
}
