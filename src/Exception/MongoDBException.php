<?php


namespace Hyperf\Mongodb\Exception;

use Exception;
use Throwable;

class MongoDBException extends Exception
{
    public function __construct($message = "", $code = 0, Throwable $previous = null)
    {
        $this->code = empty($code) ? __LINE__ : $code;
        $this->message = $message;
    }

}
