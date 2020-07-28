<?php
/**
 * Created by PhpStorm.
 * User: adamchen1208
 * Date: 2020/7/24
 * Time: 15:30
 */

namespace Hyperf\Mongodb;

use Hyperf\Mongodb\Exception\MongoDBException;
use Hyperf\Mongodb\Pool\PoolFactory;
use Hyperf\Utils\Context;

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

    public function __construct(PoolFactory $factory)
    {
        $this->factory = $factory;
    }

    /**
     * The key to identify the connection object in coroutine context.
     */
    private function getContextKey(): string
    {
        return sprintf('mongodb.connection.%s', $this->poolName);
    }

    private function getConnection()
    {
        $connection = null;
        $hasContextConnection = Context::has($this->getContextKey());
        if ($hasContextConnection) {
            $connection = Context::get($this->getContextKey());
        }
        if (!$connection instanceof MongodbConnection) {
            $pool = $this->factory->getPool($this->poolName);
            $connection = $pool->get()->getConnection();
        }
        return $connection;
    }

    /**
     * 返回满足filer的一条数据
     *
     * @param string $namespace
     * @param array $filter
     * @param array $options
     * @return array
     * @throws MongoDBException
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
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 返回满足filer的全部数据
     *
     * @param string $namespace
     * @param array $filter
     * @param array $options
     * @return array
     * @throws MongoDBException
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
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 返回满足filer的分页数据
     *
     * @param string $namespace
     * @param int $limit
     * @param int $currentPage
     * @param array $filter
     * @param array $options
     * @return array
     * @throws MongoDBException
     */
    public function findPagination(string $namespace, int $limit, int $currentPage, array $filter = [], array $options = []): array
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execFindPagination($namespace, $limit, $currentPage, $filter, $options);
        } catch (\Exception  $e) {
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 返回满足filer的一条数据（_id为自动转对象）
     *
     * @param string $namespace
     * @param array $filter
     * @param array $options
     * @return array
     * @throws MongoDBException
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
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 返回满足filer的全部数据（_id自动转对象）
     *
     * @param string $namespace
     * @param array $filter
     * @param array $options
     * @return array
     * @throws MongoDBException
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
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 返回满足filer的分页数据（_id自动转对象）
     *
     * @param string $namespace
     * @param int $limit
     * @param int $currentPage
     * @param array $filter
     * @param array $options
     * @return array
     * @throws MongoDBException
     */
    public function findPaginationId(string $namespace, int $limit, int $currentPage, array $filter = [], array $options = []): array
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execFindPaginationId($namespace, $limit, $currentPage, $filter, $options);
        } catch (\Exception  $e) {
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 插入一条数据
     *
     * @param $namespace
     * @param array $data
     * @return bool|mixed
     * @throws MongoDBException
     */
    public function insertOne($namespace, array $data = [])
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execInsertOne($namespace, $data);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 插入批量数据
     * @param $namespace
     * @param array $data
     * @return bool|string
     * @throws MongoDBException
     */
    public function insertMany($namespace, array $data)
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
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 更新数据满足$filter的行的信息成$newObject
     *
     * @param $namespace
     * @param array $filter
     * @param array $newObj
     * @return bool
     * @throws MongoDBException
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
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 只更新数据满足$filter的行的列信息中在$newObject中出现过的字段
     *
     * @param $namespace
     * @param array $filter
     * @param array $newObj
     * @return bool
     * @throws MongoDBException
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
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 更新数据满足$filter的行的信息成$newObject（_id自动转对象）
     *
     * @param $namespace
     * @param array $filter
     * @param array $newObj
     * @return bool
     * @throws MongoDBException
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
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 只更新数据满足$filter的行的列信息中在$newObject中出现过的字段（_id自动转对象）
     *
     * @param $namespace
     * @param array $filter
     * @param array $newObj
     * @return bool
     * @throws MongoDBException
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
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 删除满足条件的数据，默认只删除匹配条件的第一条记录，如果要删除多条$limit=true
     *
     * @param string $namespace
     * @param array $filter
     * @return bool
     * @throws MongoDBException
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
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 删除满足条件的数据，默认只删除匹配条件的第一条记录，如果要删除多条$limit=true
     *
     * @param string $namespace
     * @param array $filter
     * @return bool
     * @throws MongoDBException
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
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 删除满足条件的数据，默认只删除匹配条件的第一条记录，如果要删除多条$limit=true（_id自动转对象）
     *
     * @param string $namespace
     * @param array $filter
     * @return bool
     * @throws MongoDBException
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
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 返回collection中满足条件的数量
     *
     * @param string $namespace
     * @param array $filter
     * @return bool
     * @throws MongoDBException
     */
    public function count(string $namespace, array $filter = [])
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execCount($namespace, $filter);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }

    /**
     * 聚合查询
     * @param string $namespace
     * @param array $filter
     * @return bool
     * @throws MongoDBException
     * @throws \MongoDB\Driver\Exception\Exception
     */
    public function command(string $namespace, array $filter = [])
    {
        try {
            /**
             * @var $collection MongodbConnection
             */
            $collection = $this->getConnection();
            return $collection->execCommand($namespace, $filter);
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile() . $e->getLine() . $e->getMessage());
        }
    }
}
