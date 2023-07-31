<?php

namespace Hyperf\Mongodb;

!defined('BASE_PATH') && define('BASE_PATH', dirname(__DIR__));

class ConfigProvider
{
    public function __invoke(): array
    {
        return [
            'dependencies' => [
                Mongodb::class => Mongodb::class,
            ],
            'commands'     => [
            ],
            'scan'         => [
                'paths' => [
                    __DIR__,
                ],
            ],
            'publish'      => [
                [
                    'id'          => 'config',
                    'description' => 'The config of mongodb client.',
                    'source'      => __DIR__.'/../publish/mongodb.php',
                    'destination' => BASE_PATH.'/config/autoload/mongodb.php',
                ],
            ],
        ];
    }
}
