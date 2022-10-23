<?php


namespace Hyperf\Mongodb;

use Hyperf\Di\Exception\NotFoundException;
use Hyperf\Mongodb\Exception\MongoDBException;
use Hyperf\Mongodb\Pool\PoolFactory;
use Hyperf\Utils\Context;
use MongoDB\Driver\Exception\Exception;
use Psr\Container\ContainerExceptionInterface;
use Psr\Container\NotFoundExceptionInterface;

/**
 * Class Mongodb
 * @package Hyperf\Mongodb
 */
class Mongodb
{
    /**
     * @var PoolFactory
     */
    protected $factory;

    /**
     * @var string
     */
    protected $poolName = 'default';

    /**
     * Mongodb
     *
     * @param  PoolFactory  $factory
     */
    public function __construct(PoolFactory $factory)
    {
        $this->factory = $factory;
    }

    /**
     * 返回满足filer的一条数据
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @param  array  $options
     * @return array
     * @throws ContainerExceptionInterface
     * @throws MongoDBException
     * @throws NotFoundExceptionInterface
     */
    public function findOne(string $namespace, array $filter = [], array $options = []): array
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execFindOne($namespace, $filter, $options);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        }
    }

    /**
     * getConnection
     *
     * @return false|MongodbConnection|mixed|null
     * @throws NotFoundException
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    private function getConnection()
    {
        $connection           = null;
        $hasContextConnection = Context::has($this->getContextKey());
        if ($hasContextConnection) {
            $connection = Context::get($this->getContextKey());
        }
        if (!$connection instanceof MongodbConnection) {
            $pool       = $this->factory->getPool($this->poolName);
            $connection = $pool->get()->getConnection();
        }
        return $connection;
    }

    /**
     * getContextKey
     *
     * The key to identify the connection object in coroutine context.
     */
    private function getContextKey(): string
    {
        return sprintf('mongodb.connection.%s', $this->poolName);
    }

    /**
     * 返回满足filer的全部数据
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @param  array  $options
     * @return array
     * @throws ContainerExceptionInterface
     * @throws MongoDBException
     * @throws NotFoundExceptionInterface
     */
    public function findAll(string $namespace, array $filter = [], array $options = []): array
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execFindAll($namespace, $filter, $options);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        }
    }

    /**
     * 返回满足filer的分页数据
     *
     * @param  string  $namespace
     * @param  int  $limit
     * @param  int  $currentPage
     * @param  array  $filter
     * @param  array  $options
     * @return array
     * @throws ContainerExceptionInterface
     * @throws MongoDBException
     * @throws NotFoundExceptionInterface
     */
    public function findPagination(
        string $namespace,
        int $limit,
        int $currentPage,
        array $filter = [],
        array $options = []
    ): array {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execFindPagination($namespace, $limit, $currentPage, $filter, $options);
        } catch (\Exception  $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        }
    }

    /**
     * 返回满足filer的一条数据（_id为自动转对象）
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @param  array  $options
     * @return array
     * @throws ContainerExceptionInterface
     * @throws MongoDBException
     * @throws NotFoundExceptionInterface
     */
    public function findOneId(string $namespace, array $filter = [], array $options = []): array
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execFindOneId($namespace, $filter, $options);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        }
    }

    /**
     * 返回满足filer的全部数据（_id自动转对象）
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @param  array  $options
     * @return array
     * @throws ContainerExceptionInterface
     * @throws MongoDBException
     * @throws NotFoundExceptionInterface
     */
    public function fetchAllId(string $namespace, array $filter = [], array $options = []): array
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execFindAllId($namespace, $filter, $options);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        }
    }

    /**
     * 返回满足filer的分页数据（_id自动转对象）
     *
     * @param  string  $namespace
     * @param  int  $limit
     * @param  int  $currentPage
     * @param  array  $filter
     * @param  array  $options
     * @return array
     * @throws ContainerExceptionInterface
     * @throws MongoDBException
     * @throws NotFoundExceptionInterface
     */
    public function findPaginationId(
        string $namespace,
        int $limit,
        int $currentPage,
        array $filter = [],
        array $options = []
    ): array {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execFindPaginationId($namespace, $limit, $currentPage, $filter, $options);
        } catch (\Exception  $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        }
    }

    /**
     * 插入一条数据
     *
     * @param $namespace
     * @param  array  $data
     * @return bool
     * @throws ContainerExceptionInterface
     * @throws MongoDBException
     * @throws NotFoundExceptionInterface
     */
    public function insertOne($namespace, array $data = []): bool
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execInsertOne($namespace, $data);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        }
    }

    /**
     * 插入批量数据
     *
     * @param $namespace
     * @param  array  $data
     * @return array
     * @throws ContainerExceptionInterface
     * @throws MongoDBException
     * @throws NotFoundException
     * @throws NotFoundExceptionInterface
     */
    public function insertMany($namespace, array $data): array
    {
        if (count($data) == count($data, 1)) {
            throw new  MongoDBException('data is can only be a two-dimensional array');
        }
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execInsertMany($namespace, $data);
        } catch (MongoDBException $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        }
    }

    /**
     * 更新数据满足$filter的行的信息成$newObject
     *
     * @param $namespace
     * @param  array  $filter
     * @param  array  $newObj
     * @return bool
     * @throws ContainerExceptionInterface
     * @throws MongoDBException
     * @throws NotFoundExceptionInterface
     */
    public function updateRow($namespace, array $filter = [], array $newObj = []): bool
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execUpdateRow($namespace, $filter, $newObj);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        }
    }

    /**
     * 只更新数据满足$filter的行的列信息中在$newObject中出现过的字段
     *
     * @param $namespace
     * @param  array  $filter
     * @param  array  $newObj
     * @return bool
     * @throws ContainerExceptionInterface
     * @throws MongoDBException
     * @throws NotFoundExceptionInterface
     */
    public function updateColumn($namespace, array $filter = [], array $newObj = []): bool
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execUpdateColumn($namespace, $filter, $newObj);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        }
    }

    /**
     * 更新数据满足$filter的行的信息成$newObject（_id自动转对象）
     *
     * @param $namespace
     * @param  array  $filter
     * @param  array  $newObj
     * @return bool
     * @throws ContainerExceptionInterface
     * @throws MongoDBException
     * @throws NotFoundExceptionInterface
     */
    public function updateRowId($namespace, array $filter = [], array $newObj = []): bool
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execUpdateRowId($namespace, $filter, $newObj);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        }
    }

    /**
     * 只更新数据满足$filter的行的列信息中在$newObject中出现过的字段（_id自动转对象）
     *
     * @param $namespace
     * @param  array  $filter
     * @param  array  $newObj
     * @return bool
     * @throws ContainerExceptionInterface
     * @throws MongoDBException
     * @throws NotFoundExceptionInterface
     */
    public function updateColumnId($namespace, array $filter = [], array $newObj = []): bool
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execUpdateColumnId($namespace, $filter, $newObj);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        }
    }

    /**
     * 删除满足条件的数据，默认只删除匹配条件的第一条记录，如果要删除多条$limit=true
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @return bool
     * @throws ContainerExceptionInterface
     * @throws MongoDBException
     * @throws NotFoundExceptionInterface
     */
    public function deleteOne(string $namespace, array $filter = []): bool
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execDeleteOne($namespace, $filter);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        }
    }

    /**
     * 删除满足条件的数据，默认只删除匹配条件的第一条记录，如果要删除多条$limit=true
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @return bool
     * @throws ContainerExceptionInterface
     * @throws MongoDBException
     * @throws NotFoundExceptionInterface
     */
    public function deleteMany(string $namespace, array $filter = []): bool
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execDeleteMany($namespace, $filter);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        }
    }

    /**
     * 删除满足条件的数据，默认只删除匹配条件的第一条记录，如果要删除多条$limit=true（_id自动转对象）
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @return bool
     * @throws ContainerExceptionInterface
     * @throws MongoDBException
     * @throws NotFoundExceptionInterface
     */
    public function deleteOneId(string $namespace, array $filter = []): bool
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execDeleteOneId($namespace, $filter);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        }
    }

    /**
     * 返回collection中满足条件的数量
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @return int
     * @throws ContainerExceptionInterface
     * @throws MongoDBException
     * @throws NotFoundExceptionInterface
     */
    public function count(string $namespace, array $filter = []): int
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execCount($namespace, $filter);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        }
    }

    /**
     * 聚合查询
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @return bool
     * @throws ContainerExceptionInterface
     * @throws Exception
     * @throws MongoDBException
     * @throws NotFoundExceptionInterface
     */
    public function command(string $namespace, array $filter = []): bool
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execCommand($namespace, $filter);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        }
    }
}
