<?php


namespace Hyperf\Mongodb;

use Hyperf\Contract\ConnectionInterface;
use Hyperf\Mongodb\Exception\MongoDBException;
use Hyperf\Pool\Connection;
use Hyperf\Pool\Exception\ConnectionException;
use Hyperf\Pool\Pool;
use MongoDB\BSON\ObjectId;
use MongoDB\Driver\BulkWrite;
use MongoDB\Driver\Command;
use MongoDB\Driver\Exception\AuthenticationException;
use MongoDB\Driver\Exception\Exception;
use MongoDB\Driver\Exception\InvalidArgumentException;
use MongoDB\Driver\Exception\RuntimeException;
use MongoDB\Driver\Manager;
use MongoDB\Driver\Query;
use MongoDB\Driver\WriteConcern;
use Psr\Container\ContainerInterface;
use stdClass;
use Throwable;

class MongodbConnection extends Connection implements ConnectionInterface
{
    /**
     * @var Manager
     */
    protected $connection;

    /**
     * @var array
     */
    protected $config;

    /**
     * @throws MongoDBException
     */
    public function __construct(ContainerInterface $container, Pool $pool, array $config)
    {
        parent::__construct($container, $pool);
        $this->config = $config;
        $this->reconnect();
    }

    /**
     * Reconnect the connection.
     * @throws MongoDBException
     */
    public function reconnect(): bool
    {
        try {
            /**
             * http://php.net/manual/zh/mongodb-driver-manager.construct.php
             */

            $username = $this->config['username'];
            $password = $this->config['password'];
            if (!empty($username) && !empty($password)) {
                $uri = sprintf(
                    'mongodb://%s:%s@%s:%d/%s',
                    $username,
                    $password,
                    $this->config['host'],
                    $this->config['port'],
                    $this->config['db']
                );
            } else {
                $uri = sprintf(
                    'mongodb://%s:%d/%s',
                    $this->config['host'],
                    $this->config['port'],
                    $this->config['db']
                );
            }
            $urlOptions = [];
            //数据集
            $replica = $this->config['replica'] ?? null;
            if ($replica) {
                $urlOptions['replicaSet'] = $replica;
            }
            $this->connection = new Manager($uri, $urlOptions);
        } catch (InvalidArgumentException $e) {
            throw new  MongoDBException('mongodb 连接参数错误:'.$e->getMessage());
        } catch (RuntimeException $e) {
            throw new  MongoDBException('mongodb uri格式错误:'.$e->getMessage());
        }
        $this->lastUseTime = microtime(true);
        return true;
    }

    /**
     * getActiveConnection
     *
     * @throws ConnectionException
     * @throws Exception
     * @throws MongoDBException
     */
    public function getActiveConnection(): MongodbConnection
    {
        // TODO: Implement getActiveConnection() method.
        if ($this->check()) {
            return $this;
        }
        if (!$this->reconnect()) {
            throw new ConnectionException('Connection reconnect failed.');
        }
        return $this;
    }

    /**
     * 判断当前的数据库连接是否已经超时
     *
     * @return bool
     * @throws Exception
     * @throws MongoDBException
     */
    public function check(): bool
    {
        try {
            $command = new Command(['ping' => 1]);
            $this->connection->executeCommand($this->config['db'], $command);
            return true;
        } catch (Throwable $e) {
            return $this->catchMongoException($e);
        }
    }

    /**
     * @param  Throwable  $e
     * @return bool
     * @throws MongoDBException
     */
    private function catchMongoException(Throwable $e): bool
    {
        switch ($e) {
            case ($e instanceof InvalidArgumentException):
                throw new  MongoDBException('mongo argument exception: '.$e->getMessage());
            case ($e instanceof AuthenticationException):
                throw new  MongoDBException('mongo数据库连接授权失败:'.$e->getMessage());
            case ($e instanceof ConnectionException):

                /**
                 * https://cloud.tencent.com/document/product/240/4980
                 * 存在连接失败的，那么进行重连
                 */
                for ($counts = 1; $counts <= 5; $counts++) {
                    try {
                        $this->reconnect();
                    } catch (\Exception $e) {
                        continue;
                    }
                    break;
                }
                return true;

            case ($e instanceof RuntimeException):

                throw new  MongoDBException('mongo runtime exception: '.$e->getMessage());

            default:

                throw new  MongoDBException('mongo unexpected exception: '.$e->getMessage());

        }
    }

    /**
     * Close the connection.
     */
    public function close(): bool
    {
        // TODO: Implement close() method.
        return true;
    }

    /**
     * 查询返回结果的一条数据
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @param  array  $options
     * @return array
     * @throws MongoDBException
     */
    public function execFindOne(string $namespace, array $filter = [], array $options = [])
    {
        // 查询数据
        $result = [];
        try {
            $options['limit'] = 1;
            $query            = new Query($filter, $options);
            $cursor           = $this->connection->executeQuery($this->config['db'].'.'.$namespace, $query);
            foreach ($cursor as $document) {
                $result = (array) $document;
                break;
            }
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } catch (Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } finally {
            $this->pool->release($this);
            return $result;
        }
    }

    /**
     * 查询返回结果的全部数据
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @param  array  $options
     * @return array
     * @throws MongoDBException
     */
    public function execFindAll(string $namespace, array $filter = [], array $options = [])
    {
        // 查询数据
        $result = [];
        try {
            $query  = new Query($filter, $options);
            $cursor = $this->connection->executeQuery($this->config['db'].'.'.$namespace, $query);
            foreach ($cursor as $document) {
                $result[] = (array) $document;
            }
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } catch (Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } finally {
            $this->pool->release($this);
            return $result;
        }
    }

    /**
     * 返回分页数据，默认每页10条
     *
     * @param  string  $namespace
     * @param  int  $limit
     * @param  int  $currentPage
     * @param  array  $filter
     * @param  array  $options
     * @return array
     * @throws MongoDBException
     */
    public function execFindPagination(
        string $namespace,
        int $limit = 10,
        int $currentPage = 0,
        array $filter = [],
        array $options = []
    ) {
        // 查询数据
        $data   = [];
        $result = [];
        //每次最多返回10条记录
        if (!isset($options['limit']) || (int) $options['limit'] <= 0) {
            $options['limit'] = $limit;
        }
        if (!isset($options['skip']) || (int) $options['skip'] <= 0) {
            $options['skip'] = $currentPage * $limit;
        }
        try {
            $query  = new Query($filter, $options);
            $cursor = $this->connection->executeQuery($this->config['db'].'.'.$namespace, $query);
            foreach ($cursor as $document) {
                $document        = (array) $document;
                $document['_id'] = (string) $document['_id'];
                $data[]          = $document;
            }
            $result['totalCount']  = $this->execCount($namespace, $filter);
            $result['currentPage'] = $currentPage;
            $result['perPage']     = $limit;
            $result['list']        = $data;
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } catch (Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } finally {
            $this->pool->release($this);
            return $result;
        }
    }

    /**
     * 获取collection 中满足条件的条数
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @return int
     * @throws MongoDBException
     */
    public function execCount(string $namespace, array $filter = []): int
    {
        $count = 0;
        try {
            $command = new Command([
                'count' => $namespace,
                'query' => $filter
            ]);
            $cursor  = $this->connection->executeCommand($this->config['db'], $command);
            return $cursor->toArray()[0]->n;
        } catch (\Exception $e) {
            $count = 0;
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } catch (Exception $e) {
            $count = 0;
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } finally {
            $this->pool->release($this);
            return $count;
        }
    }

    /**
     * 查询返回结果的一条数据（_id自动转对象）
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @param  array  $options
     * @return array
     * @throws MongoDBException
     */
    public function execFindOneId(string $namespace, array $filter = [], array $options = []): array
    {
        if (!empty($filter['_id']) && !($filter['_id'] instanceof ObjectId)) {
            $filter['_id'] = new ObjectId($filter['_id']);
        }
        // 查询数据
        $result = [];
        try {
            $options['limit'] = 1;
            $query            = new Query($filter, $options);
            $cursor           = $this->connection->executeQuery($this->config['db'].'.'.$namespace, $query);
            foreach ($cursor as $document) {
                $document        = (array) $document;
                $document['_id'] = (string) $document['_id'];
                $result          = $document;
                break;
            }
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } catch (Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } finally {
            $this->pool->release($this);
            return $result;
        }
    }

    /**
     * 查询返回结果的全部数据（_id自动转对象）
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @param  array  $options
     * @return array
     * @throws MongoDBException
     */
    public function execFindAllId(string $namespace, array $filter = [], array $options = []): array
    {
        if (!empty($filter['_id']) && !($filter['_id'] instanceof ObjectId)) {
            $filter['_id'] = new ObjectId($filter['_id']);
        }
        // 查询数据
        $result = [];
        try {
            $query  = new Query($filter, $options);
            $cursor = $this->connection->executeQuery($this->config['db'].'.'.$namespace, $query);
            foreach ($cursor as $document) {
                $document        = (array) $document;
                $document['_id'] = (string) $document['_id'];
                $result[]        = $document;
            }
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } catch (Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } finally {
            $this->pool->release($this);
            return $result;
        }
    }

    /**
     * 返回分页数据，默认每页10条（_id自动转对象）
     *
     * @param  string  $namespace
     * @param  int  $limit
     * @param  int  $currentPage
     * @param  array  $filter
     * @param  array  $options
     * @return array
     * @throws MongoDBException
     */
    public function execFindPaginationId(
        string $namespace,
        int $limit = 10,
        int $currentPage = 0,
        array $filter = [],
        array $options = []
    ): array {
        if (!empty($filter['_id']) && !($filter['_id'] instanceof ObjectId)) {
            $filter['_id'] = new ObjectId($filter['_id']);
        }
        // 查询数据
        $data   = [];
        $result = [];
        //每次最多返回10条记录
        if (!isset($options['limit']) || (int) $options['limit'] <= 0) {
            $options['limit'] = $limit;
        }
        if (!isset($options['skip']) || (int) $options['skip'] <= 0) {
            $options['skip'] = $currentPage * $limit;
        }
        try {
            $query  = new Query($filter, $options);
            $cursor = $this->connection->executeQuery($this->config['db'].'.'.$namespace, $query);
            foreach ($cursor as $document) {
                $document        = (array) $document;
                $document['_id'] = (string) $document['_id'];
                $data[]          = $document;
            }
            $result['totalCount']  = $this->execCount($namespace, $filter);
            $result['currentPage'] = $currentPage;
            $result['perPage']     = $limit;
            $result['list']        = $data;
        } catch (\Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } catch (Exception $e) {
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } finally {
            $this->pool->release($this);
            return $result;
        }
    }

    /**
     * 插入一条数据
     * http://php.net/manual/zh/mongodb-driver-bulkwrite.insert.php
     * $data1 = ['title' => 'one'];
     * $data2 = ['_id' => 'custom ID', 'title' => 'two'];
     * $data3 = ['_id' => new MongoDB\BSON\ObjectId, 'title' => 'three'];
     *
     * @param  string  $namespace
     * @param  array  $data
     * @return string
     * @throws MongoDBException
     */
    public function execInsertOne(string $namespace, array $data = []): string
    {
        try {
            $bulk     = new BulkWrite();
            $insertId = (string) $bulk->insert($data);
            $written  = new WriteConcern(WriteConcern::MAJORITY, 1000);
            $this->connection->executeBulkWrite($this->config['db'].'.'.$namespace, $bulk, $written);
        } catch (\Exception $e) {
            $insertId = false;
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } finally {
            $this->pool->release($this);
            return $insertId;
        }
    }

    /**
     * 插入批量数据
     * http://php.net/manual/zh/mongodb-driver-bulkwrite.insert.php
     * $data = [
     * ['title' => 'one'],
     * ['_id' => 'custom ID', 'title' => 'two'],
     * ['_id' => new MongoDB\BSON\ObjectId, 'title' => 'three']
     * ];
     *
     * @param  string  $namespace
     * @param  array  $data
     * @return array
     * @throws MongoDBException
     */
    public function execInsertMany(string $namespace, array $data = []): array
    {
        $insertId = [];
        try {
            $bulk = new BulkWrite();
            foreach ($data as $items) {
                $insertId[] = (string) $bulk->insert($items);
            }
            $written = new WriteConcern(WriteConcern::MAJORITY, 1000);
            $this->connection->executeBulkWrite($this->config['db'].'.'.$namespace, $bulk, $written);
        } catch (\Exception $e) {
            $insertId = false;
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } finally {
            $this->pool->release($this);
            return $insertId;
        }
    }

    /**
     * 数据更新,效果是满足filter的行,只更新$newObj中的$set出现的字段
     * <p>
     *  <code>
     * http://php.net/manual/zh/mongodb-driver-bulkwrite.update.php
     * $bulk->update(
     *   ['x' => 2],
     *   ['$set' => ['y' => 3]],
     *   ['multi' => false, 'upsert' => false]
     * );
     * </code>
     * </p>
     * @param  string  $namespace
     * @param  array  $filter
     * @param  array  $newObj
     * @return bool
     * @throws MongoDBException
     */
    public function execUpdateRow(string $namespace, array $filter = [], array $newObj = []): bool
    {
        try {
            $bulk = new BulkWrite();
            $bulk->update(
                $filter,
                ['$set' => $newObj],
                ['multi' => true, 'upsert' => false]
            );
            $written       = new WriteConcern(WriteConcern::MAJORITY, 1000);
            $result        = $this->connection->executeBulkWrite($this->config['db'].'.'.$namespace, $bulk, $written);
            $modifiedCount = $result->getModifiedCount();
            $update        = $modifiedCount != 0;
        } catch (\Exception $e) {
            $update = false;
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } finally {
            $this->pool->release($this);
            return $update;
        }
    }

    /**
     * 数据更新, 效果是满足filter的行数据更新成$newObj
     * http://php.net/manual/zh/mongodb-driver-bulkwrite.update.php
     * $bulk->update(
     *   ['x' => 2],
     *   [['y' => 3]],
     *   ['multi' => false, 'upsert' => false]
     * );
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @param  array  $newObj
     * @return bool
     * @throws MongoDBException
     */
    public function execUpdateColumn(string $namespace, array $filter = [], array $newObj = []): bool
    {
        try {
            $bulk = new BulkWrite();
            $bulk->update(
                $filter,
                ['$set' => $newObj],
                ['multi' => false, 'upsert' => false]
            );
            $written       = new WriteConcern(WriteConcern::MAJORITY, 1000);
            $result        = $this->connection->executeBulkWrite($this->config['db'].'.'.$namespace, $bulk, $written);
            $modifiedCount = $result->getModifiedCount();
            $update        = $modifiedCount == 1;
        } catch (\Exception $e) {
            $update = false;
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } finally {
            $this->release();
            return $update;
        }
    }

    /**
     * 数据更新,效果是满足filter的行,只更新$newObj中的$set出现的字段（_id自动转对象）
     * http://php.net/manual/zh/mongodb-driver-bulkwrite.update.php
     * $bulk->update(
     *   ['x' => 2],
     *   ['$set' => ['y' => 3]],
     *   ['multi' => false, 'upsert' => false]
     * );
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @param  array  $newObj
     * @return bool
     * @throws MongoDBException
     */
    public function execUpdateRowId(string $namespace, array $filter = [], array $newObj = []): bool
    {
        try {
            if (!empty($filter['_id']) && !($filter['_id'] instanceof ObjectId)) {
                $filter['_id'] = new ObjectId($filter['_id']);
            }
            $bulk = new BulkWrite;
            $bulk->update(
                $filter,
                ['$set' => $newObj],
                ['multi' => true, 'upsert' => false]
            );
            $written       = new WriteConcern(WriteConcern::MAJORITY, 1000);
            $result        = $this->connection->executeBulkWrite($this->config['db'].'.'.$namespace, $bulk, $written);
            $modifiedCount = $result->getModifiedCount();
            $update        = $modifiedCount != 0;
        } catch (\Exception $e) {
            $update = false;
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } finally {
            $this->pool->release($this);
            return $update;
        }
    }

    /**
     * 数据更新, 效果是满足filter的行数据更新成$newObj（_id自动转对象）
     *
     * http://php.net/manual/zh/mongodb-driver-bulkwrite.update.php
     * $bulk->update(
     *   ['x' => 2],
     *   [['y' => 3]],
     *   ['multi' => false, 'upsert' => false]
     * );
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @param  array  $newObj
     * @return bool
     * @throws MongoDBException
     */
    public function execUpdateColumnId(string $namespace, array $filter = [], array $newObj = []): bool
    {
        try {
            if (!empty($filter['_id']) && !($filter['_id'] instanceof ObjectId)) {
                $filter['_id'] = new ObjectId($filter['_id']);
            }
            $bulk = new BulkWrite;
            $bulk->update(
                $filter,
                ['$set' => $newObj],
                ['multi' => false, 'upsert' => false]
            );
            $written       = new WriteConcern(WriteConcern::MAJORITY, 1000);
            $result        = $this->connection->executeBulkWrite($this->config['db'].'.'.$namespace, $bulk, $written);
            $modifiedCount = $result->getModifiedCount();
            $update        = $modifiedCount == 1;
        } catch (\Exception $e) {
            $update = false;
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } finally {
            $this->release();
            return $update;
        }
    }

    /**
     * 删除一条数据
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @return bool
     * @throws MongoDBException
     */
    public function execDeleteOne(string $namespace, array $filter = []): bool
    {
        try {
            $bulk = new BulkWrite;
            $bulk->delete($filter, ['limit' => 1]);
            $written = new WriteConcern(WriteConcern::MAJORITY, 1000);
            $this->connection->executeBulkWrite($this->config['db'].'.'.$namespace, $bulk, $written);
            $delete = true;
        } catch (\Exception $e) {
            $delete = false;
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } finally {
            $this->pool->release($this);
            return $delete;
        }
    }

    /**
     * 删除多条数据
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @return bool
     * @throws MongoDBException
     */
    public function execDeleteMany(string $namespace, array $filter = []): bool
    {
        try {
            $bulk = new BulkWrite;
            $bulk->delete($filter, ['limit' => false]);
            $written = new WriteConcern(WriteConcern::MAJORITY, 1000);
            $this->connection->executeBulkWrite($this->config['db'].'.'.$namespace, $bulk, $written);
            $delete = true;
        } catch (\Exception $e) {
            $delete = false;
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } finally {
            $this->pool->release($this);
            return $delete;
        }
    }

    /**
     * 删除一条数据（_id自动转对象）
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @return bool
     * @throws MongoDBException
     */
    public function execDeleteOneId(string $namespace, array $filter = []): bool
    {
        try {
            if (!empty($filter['_id']) && !($filter['_id'] instanceof ObjectId)) {
                $filter['_id'] = new ObjectId($filter['_id']);
            }
            $bulk = new BulkWrite;
            $bulk->delete($filter, ['limit' => 1]);
            $written = new WriteConcern(WriteConcern::MAJORITY, 1000);
            $this->connection->executeBulkWrite($this->config['db'].'.'.$namespace, $bulk, $written);
            $delete = true;
        } catch (\Exception $e) {
            $delete = false;
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } finally {
            $this->pool->release($this);
            return $delete;
        }
    }

    /**
     * 聚合查询
     *
     * @param  string  $namespace
     * @param  array  $filter
     * @return bool
     * @throws Exception
     * @throws MongoDBException
     */
    public function execCommand(string $namespace, array $filter = []): bool
    {
        try {
            $command = new Command([
                'aggregate' => $namespace,
                'pipeline'  => $filter,
                'cursor'    => new stdClass()
            ]);
            $cursor  = $this->connection->executeCommand($this->config['db'], $command);
            $count   = $cursor->toArray()[0];
        } catch (\Exception $e) {
            $count = false;
            throw new MongoDBException($e->getFile().$e->getLine().$e->getMessage());
        } finally {
            $this->pool->release($this);
            return $count;
        }
    }
}
