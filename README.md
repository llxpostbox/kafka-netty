# kafka-netty
kafka consumer encapsulation of consumption rights

最近接到一个开发任务，核心需求大概有：

  -- 1、kafka消费端需要有灵活的权限控制，如可以随时启用、停用一个consumer对某些topic的消费权限，启用状态：可以消费数据； 停用状态：不能消费受限topic的数据
  
  -- 2、需要支持分布式部署、要稳定可靠、要负载均衡

接到任务后首先是网上搜罗一圈，结果发现关于kafka数据消费权限的相关资料不多，kafka本身支持的消费权限控制的功能又不够灵活，不满足需求。但还是决定亲自试一下，参
考网络资料对kafka、zk加上消费权限控制的配置，配置并启动集群（伪分布式）成功，当运行生产者、消费者代码做测试后发现，不管是向kafka上传数据还是消费数据都变得非常慢
（比没有配置权限的时候慢），可能是版本问题、可能是伪分布式集群问题、也可能是网络问题、也有可能是docker的问题（本学期需要服务都运行在docker容器，之前没有接触过docker），
但是考虑权限用户的变动不灵活（必须重启kafka）这个大前提，所也没有去深究问题的根源。决定自己造一个轮子。

服务介绍

    -- iscas-kafka-open-platform 使用netty框架封装kafka消费者服务端
      |
       -- 1、分布式服务
	   
       -- 2、指定分区消费
	   
       -- 3、服务节点由zookeeper管理。包括服务leader管理、服务运行状况、kafka配置管理、消费者数据同步
	   
       -- 4、消费者（用户）权限验证与消费数据控制
    
    -- iscas-kafka-open-platform-client 客户端
      |
      -- 1、在线服务选择，随机负载均衡
	  
      -- 2、指定分区、偏移消费数据
      
到此基本功能就实现完成，当准备开发服务端负载均衡时，接到需求变化通知，由于不可抗拒原因，该功能先砍掉， 后期待定...... 

总结： 需求基本实现，目前服务任部署到测试服务器，运行测试2周时间，功能和稳定基本没有问题。 
      
      未完成的部分有： 服务端负载均衡、 用户注册与消费者权限管理功能开发
