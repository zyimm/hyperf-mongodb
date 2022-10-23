<?php


namespace Hyperf\Mongodb\Pool;

use Hyperf\Di\Container;
use Hyperf\Di\Exception\NotFoundException;
use Psr\Container\ContainerExceptionInterface;
use Psr\Container\ContainerInterface;
use Psr\Container\NotFoundExceptionInterface;
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
     * getPool
     *
     * @param  string  $name
     * @return MongoDBPool|mixed|Channel
     * @throws NotFoundException
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    public function getPool(string $name)
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
