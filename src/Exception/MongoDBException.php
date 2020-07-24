<?php
/**
 * Created by PhpStorm.
 * User: adamchen1208
 * Date: 2020/7/24
 * Time: 15:25
 */

namespace Hyperf\Mongodb\Exception;

class MongoDBException extends \Exception
{
    /**
     * @param string $msg
     * @throws MongoDBException
     */
    public static function managerError(string $msg)
    {
        throw new self($msg);
    }
}
