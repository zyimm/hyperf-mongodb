# hyperf-mongodb

#### 介绍

用于hyperf的mongodb连接池组件，暂不支持协程。基于 (Adam/hyperf-mongodb)[https://gitee.com/adamchen1208/hyperf-mongodb?_from=gitee_search],
改作者好像已经不更新了，且源代码有些bug，依赖也存在一些问题。
1. 为此我clone该项目进行大幅度的修改，和原先项目变动较大，所以另起项目进行开源！
2. 增加单元测试，优化依赖。
3. 规范代码编写，简洁了源代码中一些不规范，重复高的代码。

# hyperf mongodb pool

```
## step 1
{
  "repositories": [{
    "type": "composer",
    "url": "http://composer.zyimm.com"
  }]
}

## step 2

composer require zyimm/hyperf-mongodb
```

## config

在/config/autoload目录里面创建文件 mongodb.php 添加以下内容

```php
return [
    'default' => [
             'username' => env('MONGODB_USERNAME', ''),
             'password' => env('MONGODB_PASSWORD', ''),
             'host' => env('MONGODB_HOST', '127.0.0.1'),
             'port' => env('MONGODB_PORT', 27017),
             'db' => env('MONGODB_DB', 'test'),
             'authMechanism' => 'SCRAM-SHA-256',
             //设置复制集,没有不设置
             //'replica' => 'rs0',
            'pool' => [
                'min_connections' => 3,
                'max_connections' => 1000,
                'connect_timeout' => 10.0,
                'wait_timeout' => 3.0,
                'heartbeat' => -1,
                'max_idle_time' => (float) env('MONGODB_MAX_IDLE_TIME', 60),
            ],
    ],
];
```

# 使用案例

使用注解，自动加载
**\Hyperf\Mongodb\Mongodb**

```php
/**
 * @Inject()
 * @var Mongodb
*/
 protected $mongodb;
```

#### **tips:**

查询的值，是严格区分类型，string、int类型的哦

### 查询一条数据

```php
$where = ['_id' => '1'];
$result = $this->$mongodb->findOne('test', $where);
```

### 查询全部数据

```php
$where = ['_id' => '1'];
$result = $this->$mongodb->findAll('test', $where);
```

### 分页查询

```php
$list = $this->$mongodb->findPagination('article', 10, 0, ['author' => $author]);
```

### 查询一条数据（_id自动转对象）

```php
$where = ['_id' => '1'];
$result = $this->$mongodb->findOneId('test', $where);
```

### 查询全部数据（_id自动转对象）

```php
$where = ['_id' => '1'];
$result = $this->$mongodb->findAllId('test', $where);
```

### 分页查询（_id自动转对象）

```php
$list = $this->$mongodb->findPaginationId('article', 10, 0, ['author' => $author]);
```

### 插入一条数据

```php
$insert = [
            '_id' => '',
            'password' => ''
];
$this->$mongodb->insert('test',$insert);
```

### 插入批量数据

```php
$insert = [
            [
                '_id' => '',
                'password' => ''
            ],
            [
                '_id' => '',
                'password' => ''
            ]
];
$this->$mongodb->insertAll('test',$insert);
```

### 更新

```php
$where = ['_id'=>'1112313423'];
$updateData = [];

$this->$mongodb->updateColumn('test', $where,$updateData); // 只更新数据满足$where的行的列信息中在$newObject中出现过的字段
$this->$mongodb->updateRow('test',$where,$updateData);// 更新数据满足$where的行的信息成$newObject
```

### 更新（_id自动转对象）

```php
$where = ['_id'=>'1112313423'];
$updateData = [];

$this->$mongodb->updateColumnId('test', $where,$updateData); // 只更新数据满足$where的行的列信息中在$newObject中出现过的字段
$this->$mongodb->updateRowId('test',$where,$updateData);// 更新数据满足$where的行的信息成$newObject
```

### 删除

```php
$where = ['_id'=>'1112313423'];
$all = true; // 为false只删除匹配的一条，true删除多条
$this->$mongodb->deleteOne('test',$where,$all);
```

### 批量删除

```php
$where = ['_id'=>'1112313423'];
$all = true; // 为false只删除匹配的一条，true删除多条
$this->$mongodb->deleteMany('test',$where,$all);
```

### 删除（_id自动转对象）

```php
$where = ['_id'=>'1112313423'];
$all = true; // 为false只删除匹配的一条，true删除多条
$this->$mongodb->deleteOneId('test',$where,$all);
```

### 统计

```php
$filter = ['isGroup' => "0", 'wechat' => '15584044700'];
$count = $this->$mongodb->count('test', $filter);
```

### 聚合查询

**sql** 和 **mongodb** 关系对比图

|   SQL  | MongoDb |
| --- | --- |
|   WHERE  |  $match (match里面可以用and，or，以及逻辑判断，但是好像不能用where)  |
|   GROUP BY  | $group  |
|   HAVING  |  $match |
|   SELECT  |  $project  |
|   ORDER BY  |  $sort |
|   LIMIT  |  $limit |
|   SUM()  |  $sum |
|   COUNT()  |  $sum |

```php

$pipeline= [
            [
                '$match' => $where
            ], [
                '$group' => [
                    '_id' => [],
                    'groupCount' => [
                        '$sum' => '$groupCount'
                    ]
                ]
            ], [
                '$project' => [
                    'groupCount' => '$groupCount',
                    '_id' => 0
                ]
            ]
];

$count = $this->$mongodb->command('test', $pipeline);
```
