# HotIPAnalysis

模块介绍：电商网站的运营过程中，分析排名前几位的IP信息，主要用来进行审计：是否是异常IP、黑名单IP。

需求分析：
①　如何统计IP
1)	通过用户点击日志，进行实时计算

②　如何分析IP
1)	分析不同时段IP的访问量
2)	分析热门IP

技术实现
基于Storm和Spark Streaming


