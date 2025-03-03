## NexusDB Core

Once demo article is accepted, we will opensource all the source code of NexusDB implementation. Experiments can be made to compare with other TSDBs.

1. 根据sqlite改造成自己的后端，client与server同体的
  1.1 读懂人家代码（完成40%）
  1.2 替换具体实现（Index Manager不单独拆分，也不加Worker Pool）
2. 完成各类测试，直接应用于具体项目
3. 添加Worker Pool