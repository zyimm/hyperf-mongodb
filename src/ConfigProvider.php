<?php
/**
 * Created by PhpStorm.
 * User: adamchen1208
 * Date: 2020/7/24
 * Time: 15:29
 */

namespace Hyperf\Mongodb;

use Hyperf\Mongodb\MongoDb;

class ConfigProvider
{
    public function __invoke(): array
    {
        return [
            'dependencies' => [
                MongoDb::class => MongoDb::class,
            ],
            'commands' => [
            ],
            'scan' => [
                'paths' => [
                    __DIR__,
                ],
            ],
            'publish' => [
                [
                    'id' => 'config',
                    'description' => 'The config of mongodb client.',
                    'source' => __DIR__ . '/../publish/mongodb.php',
                    'destination' => BASE_PATH . '/config/autoload/mongodb.php',
                ],
            ],
        ];
    }
}
