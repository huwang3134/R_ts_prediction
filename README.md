# 时间序列VV预测模型说明

## 输入
  postgreSQL和MySQL数据库抽取的视频合集每日/每小时VV(Video Visit)数

## 输出
  
 - 绘图输出data/png  
 - 模型结果汇总data/stats

## 预测变量
本节主要描述除了VV历史值以外的其它外部预测变量，统计上又称协变量(*covariates*)。本文中使用的模型除了*baggedETS*和*TBATS*模型以外均支持添加外部预测变量。
不同的预测模型可能使用不同的预测变量组合，因为除了*GAM*和*nnetar*以外的大部分模型都是使用线性回归(*Ordinary Least Squares, OLS*)来拟合外部预测变量，存在预测特征矩阵缺秩的情况，因此需要使用方差膨胀因子(*Variance inflation factor, VIF*)进行矩阵降维处理。

### 每日/每周 周期性波动
下图黑色曲线为使用傅里叶级数拟合出来的iPhone端“歌手2018”合集上线1-15天的每小时VV周期性波动趋势， 红色为真实每小时VV值。 
![image](https://user-images.githubusercontent.com/3760475/39611463-64c93858-4f8a-11e8-8b1a-f98a3bd6b250.png)  
上图可以看出来使用傅里叶级数的线性模型在该合集上的拟合效果并不好， 因此单一使用傅里叶级数拟合的每日/每周趋势作为预测变量的线性模型在波动剧烈的合集上的效果并不会太好。

### 合集更新时间
下图中红线标注的为合集更新时间, 上图为合集每小时VV，下图为合集每小时VV的变化率。  
![image](https://user-images.githubusercontent.com/3760475/39611469-6ebd344a-4f8a-11e8-9e24-fcd81ed96f5b.png)     
### 观看时间的组合特征
利用观看小时和观看工作日组合编码, 作为新的特征。  
![image](https://user-images.githubusercontent.com/3760475/39611472-73d1b8d4-4f8a-11e8-8c5b-6736827ace0c.png)
  
上图为使用GAM模型拟合的iPhone端"恋爱先生"VV在观看时间维度(观看时间/观看工作日)的分布, 可以看出来, 该节目观看高峰主要是周末22-23点这个时间段。  

### 公共节假日
该类变量取值为0或者1, 又称伪变量。 包括是否国家法定节假日, 工作日, 观看时间。 其中公共节假日信息从 https://www.timeanddate.com/holidays/china/ 获取。值得注意的是预测模型测试VV数据中包含了春节节假期间的VV数据,并且很多节目在春节期间停止了常规更新。

### 异常值标注
下图为iphone端“歌手2018”节目开播3-15天的每小时异常值标注结果，其中上图红色点为*tsoutliers*算法检测出来的异常值， 蓝色曲线为调整之后的VV曲线， 下图为异常值对原时间序列VV值的影响程度。  
    ![image](https://user-images.githubusercontent.com/3760475/39611488-99baf358-4f8a-11e8-86c3-0c7cfc017a12.png)
经过对比测试，经过异常值标注以后的VV训练集预测效果有较明显提升，但该算法速度很慢，因此该类预测变量仅在每日VV预测模型中使用。

## 使用模型简介

 - *tslm* 时间序列线性模型. 实际测试效果很差， 故测试结果没有列出。
 -  *ARIMAX* 全称Regression with ARIMA Errors, 预测原理为先通过外部变量做线性回归, 线性回归无法解释的部分再通过历史值加历史预测误差进行拟合。
 - *NNETAR*  单层前向神经网络. 使用响应变量/Y的历史值以及外部预测变量作为输入, 即非线性的特殊结构的AR(p)模型.
 -  *GAM*  全称Generalized Additive Model, 通过带惩罚项的样条函数(Spline)拟合季节性波动和趋势性波动。 该方法以及相应的mgcv库由布里斯托大学统计系教授Simon
   Wood开发。
 -  *TBATS* 混合模型, 先通过指数平滑法(ExponenTial Smoothing, ETS)拟合Y值， 拟合的误差部分再通过ARMA模型拟合, 支持多季节性, 以及非固定周期长度的时间序列数据.
 -   *baggedETS* 先对样本集按照Moving Block Bootstrapping方法(定义参见附录)重新抽样,对重新采样的K个样本集拟合ETS模型, 最后取K个ETS模型拟合的均值作为预测结果
 -  *bsts*(Bayesian Structural Time Series)  贝叶斯结构时间序列模型。假定VV值都是由一个未知分布产生， 通过已知训练集和马氏链蒙特卡洛(MCMC)算法模拟该未知分布。
   预测过程就是从求出的条件后验分布中抽样N次取均值的过程。*bsts*库由哈佛大学统计系的Steven Scott开发.
   上述模型除了*GAM*和*bsts*以外均由莫纳什大学统计系的**Rob Hyndman**教授开发，他也是时间序列预测领域最权威的学者之一。

## 不同类型预测模型比较

| 模型名称        | 支持加入外部预测变量           | 支持多周期  | 训练速度 | 周期长度可变 | 多样本集采样 | "冷启动"速度 |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
|*TBATS*    |否  |是  | 快 |   是 | 否 | 很慢|
|*baggedETS* |  否   | 是 |   很慢 |    是 | 是   | 慢 |
|*ARIMAX*   |是  | 是* |  很慢 |    否   | 否 |   慢|
|*NNETAR*   |是  | 是* |  快   |否 |    否 | 较快|
|*GAM* |    是 | 是* |    快 | 否 | 否 | 较快|
|*tslm* |是  | 是* |  快 | 否   | 否 | 较慢|
|*bsts* |是  | 是* |  较慢 |    否   | 否 | 较快|

## 预测流程
![image](https://user-images.githubusercontent.com/3760475/39611499-a68f0970-4f8a-11e8-8dc3-b2e85cdb6f46.png)
![image](https://user-images.githubusercontent.com/3760475/39611512-bd399532-4f8a-11e8-8c83-e2125b7c5bd8.png)

## 结果比较
 ![image](https://user-images.githubusercontent.com/3760475/39611718-7a831856-4f8c-11e8-8f30-b4167b8e7c31.png)
    从测试结果来看， 抽样方法/模型集成方法比模型更重要， iPhone端在TOP 30合集中预测表现最好也最稳定的模型是最简单的、没有使用任何外部预测变量的ETS模型，因为其使用了*MBB*抽样这种模型集成方法(*bagging*)使预测值变得更加平滑和健壮。   
    具体内容请参见[文件](https://github.com/vcbin/R_ts_prediction/blob/master/%E5%90%88%E9%9B%86VV%E9%A2%84%E6%B5%8B%E6%A8%A1%E5%9E%8B%E8%AF%B4%E6%98%8E.docx) 

标签（空格分隔）： 时间序列 预测模型 forecast R 
