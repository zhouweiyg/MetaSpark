package com.ynu.common

import java.util.HashSet
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import java.util.ArrayList
import redis.clients.jedis.JedisShardInfo
import redis.clients.jedis.HostAndPort

/**
 * Redis相关设置、操作
 */
object RedisClientFactory extends Serializable {

  val sentinelClusterName = "mymaster"

  // Sentinel 模式，需要启动几个Sentinel，随便几个Redis Server，Sentinel自动选举master
  val sentinelServerHostHS = new HashSet[String]
  sentinelServerHostHS.add("192.168.1.103:26379")
  sentinelServerHostHS.add("192.168.1.104:26379")
  sentinelServerHostHS.add("192.168.1.105:26379")
  sentinelServerHostHS.add("192.168.1.106:26379")
  sentinelServerHostHS.add("192.168.1.107:26379")
  sentinelServerHostHS.add("192.168.1.108:26379")

  // Standalone 模式，需要启动几个Redis Server，设置Master
  val shardsServerHostList = new ArrayList[JedisShardInfo]
  val si1 = new JedisShardInfo("192.168.1.111", 6379)

  shardsServerHostList.add(si1)

  // JedisCluster 模式，需要启动所有机器，配置文件中为cluster model
  val jedisClusterNodes = new java.util.HashSet[HostAndPort]()
  jedisClusterNodes.add(new HostAndPort("192.168.1.100", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.1.101", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.1.103", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.1.104", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.1.105", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.1.106", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.1.107", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.1.108", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.1.109", 6379))

  // 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)，默认值是8。
  val MAX_ACTIVE = -1;
  // 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
  val MAX_IDLE = -1;
  // 等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
  val MAX_WAIT = -1;
  // 超时时间，0永不超时
  val TIMEOUT = 0;

  var poolConfig = new GenericObjectPoolConfig
  poolConfig.setMaxTotal(MAX_ACTIVE)
  poolConfig.setMaxIdle(MAX_IDLE)
  poolConfig.setMaxWaitMillis(MAX_WAIT)
  poolConfig.setTestOnBorrow(true)
  poolConfig.setTestOnReturn(true)

  def getPoolConfig: GenericObjectPoolConfig = poolConfig

  //  lazy val jedisSentinelPool = new JedisSentinelPool("mymaster", sentinelServerHostHS, poolConfig, TIMEOUT)
  //  lazy val jedisShardedPool = new ShardedJedisPool(poolConfig, shardsServerHostList)
  //  lazy val sentinelConnection = jedisSentinelPool.getResource
  //  lazy val shardedConnection = jedisShardedPool.getResource

}