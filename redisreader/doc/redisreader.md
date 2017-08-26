# DataX RedisReader 说明


------------

## 1 快速介绍

RedisReader提供了读取redis dump.rdb的能力。


## 2 功能与限制

RedisReader实现了从dump.rdb文件读取数据并转为DataX协议的功能，由于要将redis无结构化的数据转换为关系型行式数据，目前RedisReader有如下限制：

1. 需要提供js代码片段或js文件,该文件必须提供map,reduce函数处理redis数据。

2. map函数接受(key,value)函数输入,其中key是string类型,value根据redis key类型,如果是简单k-v,则value是string,如果是hash则是{"string":"string"}hash对,如果是set或list则是["string","string"...], map函数里调用mr.collect(newkey,property,property_value)收集转化的新值。

3. reduce函数的输入是map聚集的结果(newkey,property_obj),即:如果map里collect('a',"name","jack"), collect('a','age',23),那么reduce输入就是('a',{"name":"jack","age":23}); reduce函数输出一行数据最终流入到datax,reduce里调用mr.addrow([val1,val2,val3..])向datax写入一行数据。


## 3 功能说明


### 3.1 配置样例

```json
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "redisreader",
                    "parameter": {
                        "dumpfile": ["/tmp/dump.rdb"],
                        "keyMatch": "*",
                        "mapreduceJs": "function map(key,raw_value){for(var p in raw_value){mr.collect(key,p,raw_value[p]);}} function reduce(newkey,obj){mr.addrow([newkey,obj['name'],obj['age']]);}",
                    }
                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": true,
                        "encoding": "UTF-8"
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": 1
            }
        }
    }
}
```

### 3.2 参数说明

* **dumpfile**

	* 描述：redis的dump.rdb文件路径。 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **keyMatch**

	* 描述：需要导入的redis key值,默认全部*。 <br />

	* 必选：是 <br />

	* 默认值：全部按照string类型读取 <br />

* **db**

	* 描述：只抽取特定db <br />

	* 必选：否 <br />

	* 默认值：, <br />

* **mapreduceJs**

	* 描述：map-reduce函数js。 <br />

	* 必选：mapreduceJs/mapreduceJsFile必须定义一个 <br />

 	* 默认值：无 <br />

* **mapreduceJsFile**

	* 描述：map-reduce函数定义所在js文件。<br />

	* 必选：mapreduceJs/mapreduceJsFile必须定义一个 <br />

 	* 默认值：无 <br />


