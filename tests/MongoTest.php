<?php
namespace Zyimm\Test;


use PHPUnit\Framework\TestCase;


class MongoTest extends TestCase
{
    public function testFindALL()
    {    
        $this->assertArrayHasKey('error_code', []);
    }
}