# <center>金融大数据实验3报告</center>
<center><font size=2>161278037 肖扬</font></center>


## 一.需求1

### 1.目的
>针对股票新闻数据集中的新闻标题，编写WordCount程序，统计所有除Stop-word（如“的”，“得”，“在”等）出现次数k次以上的单词计数，最后的结果按照词频从高到低排序输出。

### 2.设计思路
需要两次mapreduce，分别为得到词频和对结果排序。

总体上与wordcount程序类似，仅需在原来的基础上加入分词和按照词频从大到小排序即可。

其中分词可以通过调用**word-1.3.1.jar**实现。

排序通过一个mapreduce程序，调用hadoop自带的**InverseMapper**,并自定义一个comparator类用于对IntWritable型的key进行排序。

### 3.程序流程及主要函数（类）说明
（1）主要函数（类）：
```java
public static class NewsWCMapper extends Mapper<LongWritable,Text,Text,IntWritable>
```
NewsWCMapper实现按行读取后对标题进行分词功能，结果输出为<词，1>

```java
public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
```
IntSumReducer实现对map结果进行求和功能，结果输出为<词，词频>，且只有词频大于预先给定的K才输出。

```java
private static class IntWritableDecreasingComparator extends IntWritable.Comparator
```
该类实现对IntWritable按从大到小排序功能。

（2）流程

①NewsWCMapper

②IntSumReducer

③InverseMapper

④设定IntWritableDecreasingComparator为排序类对map的结果排序并输出

⑤hadoop默认reducer输出对map排序后的结果

### 4.补充说明
（1）为简化问题，对每行文本分词之前通过
```java
private String pattern = "[^\u4e00-\u9fa5]"；
line = line.replaceAll(pattern, "")；
```
去除所有非汉字字符。

（2）InverseMapper实现了键值互换功能。

（3）stop-word文件加入到word分词器的conf中，其在分词的过程中自动处理了这些停词。

（4）word-1.3.1.jar直接加入到hadoop的lib中，以解决NoClassDefFoundError问题。

### 5.文件说明
NewsWC.java为源代码，newswc.jar为执行程序，newswcoutput为运行结果。

## 二.需求2
### 1.目的
>针对股票新闻数据集，以新闻标题中的词组为key，编写带URL属性和词频的文档倒排索引程序，并按照词频从大到小排序，将结果输出到指定文件。输出格式可以如下：
高速公路， 10， 股票代码，[url0, url1,...,url9]
高速公路， 8， 股票代码，[url0, url1,...,url7]

### 2.设计思路
总体思路类似于NewsWC，包括两个mapreduce：

（1）第一部分的mapreduce应该实现将所有的（词，股票，词频，[urls]）输出。

map结果应为<词+股票，url+1>,reduce对后者进行合并,并将输出结果调整为<词+词频,股票+urls>

（2）第二部分的mapreduce对第一部分的结果进行排序，对所有词分别对词频排序。

map读出第一部分结果，此处将键修改为一个新的WritableComparable类，输出即可，无需reduce，排序通过重写compareTo方法

### 3.函数流程与主要函数（类）说明
（1）主要类
```java
public static class NewsWC2Mapper extends Mapper<LongWritable,Text,Text,Text>
```
将每行数据按照分隔符划分，取出标题，股票代码和url，对标题进行分词，输出键值对<词+股票,url+1>。

map中为满足不同文件格式，先将"\t"替换成"  "再split，处理长度为5和6的结果，其余情况为无效数据，忽略即可。

```java
public static class NewsWC2Reducer extends Reducer<Text,Text,Text,Text>
```
对传入的键值对合并，计算每个词在每支股票中出现的总次数，并调整输出结果为<词+词频，股票+urls>

```java
public static class SortMapper extends Mapper<Text,Text,NewText,Text>
```
排序map，将前一mapreduce中Text形式的key转换为能按照排序要求自动排序的NewText，NewText定义如下

```java
public class NewText implements WritableComparable<NewText>{
  private String a;
  private int fre;
  public NewText() {}
  public void set(String line)
  public String geta()
  public int getfre()
  public String toString()
  public void readFields(DataInput in) throws IOException
  public void write(DataOutput out) throws IOException
  public int compareTo(NewText o)
}
```
compareTo(NewText)为对NewText类型比较大小，规则为优先比较词(a),词相同时词频(fre)大者小。

set(String)为根据符合规则的字符串生成相应的NewText类，toString()限制在文件中写入NewText的格式

(2)流程

①NewsWC2Reducer

②NewsWC2Reducer

③SortMapper

## 4.相关文件
newswc2output为运行结果；

newswc2.jar为执行程序；

NewsWC2.java和NewText为源代码。
