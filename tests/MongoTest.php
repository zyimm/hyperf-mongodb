<?php
namespace Zyimm\Test;


class MongoTest extends \PHPUnit\Framework\TestCase
{
    public function testFindALL()
    {
        $this->assertArrayHasKey('error_code', Sdk::instance($this->config())
            ->service()
            ->memberInfo()->getMemberList());

    }
}