<?php

namespace Hyperf\Mongodb\Pool;

use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\ConnectionInterface;
use Hyperf\Mongodb\Exception\MongoDBException;
use Hyperf\Mongodb\MongodbConnection;
use Hyperf\Pool\Pool;
use Hyperf\Utils\Arr;
use InvalidArgumentException;
use Psr\Container\ContainerInterface;

class MongoDBPool extends Pool
{
    /**
     * @var string
     */
    protected $name;

    /**
     * @var array
     */
    protected $config;

    public function __construct(ContainerInterface $container, string $name)
    {
        $this->name = $name;
        $config     = $container->get(ConfigInterface::class);
        $key        = sprintf('mongodb.%s', $this->name);
        if (!$config->has($key)) {
            throw new InvalidArgumentException(sprintf('config[%s] is not exist!', $key));
        }

        $this->config = $config->get($key);
        $options      = Arr::get($this->config, 'pool', []);

        parent::__construct($container, $options);
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * createConnection
     *
     * @throws MongoDBException
     */
    protected function createConnection(): ConnectionInterface
    {
        return new MongodbConnection($this->container, $this, $this->config);
    }
}
