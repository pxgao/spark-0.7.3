package spark.streaming.examples

import spark.streaming.{Time, Seconds, DStream, StreamingContext}
import spark.RDD
import scala.actors.Actor._
import scala.actors.Actor
import scala.collection.immutable
import scala.io._
import java.io.{File, PrintWriter}
import java.util.Calendar

/**
 * Created with IntelliJ IDEA.
 * User: peter
 * Date: 10/13/13
 * Time: 8:25 PM
 * To change this template use File | Settings | File Templates.
 */
class SqlSparkStreamingContext(_ssc : StreamingContext) {
  val ssc = _ssc
  val inputStreams = scala.collection.mutable.Map[String, DStream[String]]()
  val recentBatchOfInputStreams = scala.collection.mutable.Map[Time, scala.collection.mutable.Map[String, RDD[String]]]()
  val operatorGraph = new OperatorGraph(this)
  val tables = scala.collection.mutable.Map[String, Table]()
  val parser = new SqlParser()
  var args :Array[String] = null


  object columns {
    private var globalColumnCount = 0
    def getGlobalColId = {globalColumnCount += 1; globalColumnCount - 1}
  }

  def socketTextStream(ip : String, port : Int, name: String) {
    inputStreams += name -> ssc.socketTextStream(ip, port)
  }


  def start(){
    operatorGraph.groupInnerJoin();

    inputStreams.foreach(kvp => {
      val name = kvp._1
      val stream = kvp._2
      stream.foreach((rdd,time) => {
        if (!recentBatchOfInputStreams.contains(time))
          recentBatchOfInputStreams += time -> scala.collection.mutable.Map[String, RDD[String]]()
        recentBatchOfInputStreams(time) += name -> rdd
        if(recentBatchOfInputStreams(time).keySet == inputStreams.keys){
          //processRDDActor ! (time,recentBatchOfInputStreams(time))
          processBatch(time, recentBatchOfInputStreams(time))
          recentBatchOfInputStreams - time
        }
      })
    })
    ssc.start()
  }

  def stop(){
    ssc.stop()
  }




//  val processRDDActor = actor{
//    while(true){
//      receive{
//        case (time:Time, rdds :scala.collection.mutable.Map[String, RDD[String]]) =>
//        {
//          processBatch(time,rdds)
//        }
//      }
//    }
//  }

  var timeSum = 0.0
  var batchCount = 0
  var usedCount = 0
  def processBatch(time:Time, rdds :scala.collection.mutable.Map[String, RDD[String]]){
    if(args.length > 2 && args(2) == "-o")
      operatorGraph.innerJoinOperatorSets.foreach(s => s.optimize())
    println("running " + time)
    val starttime = System.nanoTime()
    val exec = new Execution(time,rdds)
    operatorGraph.execute(SqlHelper.printRDD,exec)
    val timeUsed = (System.nanoTime() - starttime)/1000000.0
    if(batchCount > 10){
      timeSum += timeUsed
      usedCount += 1
    }
    batchCount += 1
    println("execution time in ms:" + timeUsed + " Avg:" + (timeSum/usedCount))
    SqlHelper.writeln(timeUsed.toString)
    operatorGraph.innerJoinOperators.foreach(println(_))
  }


  def executeQuery(q : Any) = q match{
    case (t:Identifier, s:SelectStatement) => {
      executeSelectQuery(t,s)
    }
    case (t:Identifier, s:InputStatement) => {
      socketTextStream(s.ip, s.port, t.name + "_input")
      val nameTypeGID = s.column.map(tp => (tp._1,(tp._2, columns.getGlobalColId))).toMap
      val schema = new Schema(nameTypeGID.values.toIndexedSeq)
      val operator = new ParseOperator(schema, s.dilimiter, t.name + "_input",this)
      tables += t.name -> new Table(nameTypeGID, operator)
    }
    case o:OutputStatement =>{
      new OutputOperator(tables(o.table).getSinkOperator, tables(o.table).getTypeGIDFromName.map(tp => tp._2._2).toIndexedSeq, this)
    }
    case c:Comment => {}
  }



  def executeSelectQuery(t : Identifier, s: SelectStatement){
    def getFunctionFromName(name : String, typeName : String) : GroupByCombiner = {
      //createCombiner: V => C,
      //mergeValue: (C, V) => C,
      //mergeCombiners: (C, C) => C,
      //final processing: C => U
      typeName match{
        case "int" => {
          name match{
            case "sum" => new GroupByCombiner(
              ((a : Int) => a),
              ((a:Int, b:Int) => a + b),
              ((a:Int, b:Int) => a + b),
              ((a : Int) => a),
              "int")
            case "max" =>  new GroupByCombiner(
              ((a : Int) => a),
              ((a:Int, b:Int) => scala.math.max(a,b)),
              ((a:Int, b:Int) => scala.math.max(a,b)),
              ((a : Int) => a),
              "int")
            case "min" =>  new GroupByCombiner(
              ((a : Int) => a),
              ((a:Int, b:Int) => scala.math.min(a,b)),
              ((a:Int, b:Int) => scala.math.min(a,b)),
              ((a : Int) => a),
              "int")
            case "avg" =>  new GroupByCombiner(
              ((a : Int) => (a,1)),
              ((t : Tuple2[Int,Int], b:Int) => (t._1 + b ,t._2 + 1)),
              ((t1:Tuple2[Int,Int], t2:Tuple2[Int,Int]) => (t1._1 + t2._1, t1._2 + t2._2)),
              ((t : Tuple2[Int,Int]) => t._1.toDouble/t._2),
              "double")


          }
        }
        case "double" => {
          name match{
            case "sum" =>  new GroupByCombiner(
              ((a : Double) => a),
              ((a:Double, b:Double) => a + b),
              ((a:Double, b:Double) => a + b),
              ((a : Double) => a),
              "double")
            case "max" =>  new GroupByCombiner(
              ((a : Double) => a),
              ((a:Double, b:Double) => scala.math.max(a,b)),
              ((a:Double, b:Double) => scala.math.max(a,b)),
              ((a : Double) => a),
              "double")
            case "min" =>  new GroupByCombiner(
              ((a : Double) => a),
              ((a:Double, b:Double) => scala.math.min(a,b)),
              ((a:Double, b:Double) => scala.math.min(a,b)),
              ((a : Double) => a),
              "double")
            case "avg" =>  new GroupByCombiner(
              ((a : Double) => (a,1)),
              ((t : Tuple2[Double,Int], b:Double) => (t._1 + b ,t._2 + 1)),
              ((t1:Tuple2[Double,Int], t2:Tuple2[Double,Int]) => (t1._1 + t2._1, t1._2 + t2._2)),
              ((t : Tuple2[Double,Int]) => t._1/t._2),
              "double")
          }
        }
      }
    }

    val sql = s.sql
    val fromTable = tables(sql("from").asInstanceOf[Identifier].name)
    var tailOperator = fromTable.getSinkOperator

    var getTypeGIDFromName = fromTable.getTypeGIDFromName
    val joinedTables = scala.collection.mutable.Map[String, Table](sql("from").asInstanceOf[Identifier].name -> fromTable)

    if(sql.contains("where"))
    {
      val condition = sql("where").asInstanceOf[Condition]
      val whereColName = condition.GetVariableSet()
      val whereColGID = whereColName.map(name => fromTable.getTypeGIDFromName(name)._2)
      val getColumnGIdFromName = fromTable.getTypeGIDFromName.filter(tp => whereColName(tp._1)).map(tp => (tp._1, tp._2._2))
      val f = (record : IndexedSeq[Any], schema : Schema) => {
        val _condition : Condition = condition
        val _getColumnLocalIdFromName = getColumnGIdFromName.map(tp => (tp._1, schema.getLocalIdFromGlobalId(tp._2)))
        condition.Eval(_getColumnLocalIdFromName, record)
      }
      tailOperator = new WhereOperator(tailOperator, f, whereColGID, this)
    }
    if(sql.contains("window")){
      tailOperator = new WindowOperator(tailOperator, sql("window").asInstanceOf[WindowProperty].windowDuration, this)
    }
    if(sql.contains("groupby") ||
      sql("select").asInstanceOf[List[SelectItem]]
        .map(si => if(si.function != null) 1 else 0)
        .reduce(_+_) > 0)
    {
      val keys = sql("select").asInstanceOf[List[SelectItem]].filter(si => si.function == null).map(si => fromTable.getTypeGIDFromName(si.selectCol.name)._2).toIndexedSeq
      assert(!sql.contains("groupby") ||
        keys.toSet.equals(
        sql("groupby").asInstanceOf[List[Identifier]].map(i => fromTable.getTypeGIDFromName(i.name)._2).toSet)
      )




      val functions = sql("select").asInstanceOf[List[SelectItem]].filter(i => i.function != null).map(i => (getTypeGIDFromName(i.selectCol.name)._2, getFunctionFromName(i.function.name, getTypeGIDFromName(i.selectCol.name)._1))).toMap
      val getNewNameFromOldGID = sql("select").asInstanceOf[List[SelectItem]].filter(i => i.function != null).map(i => (fromTable.getTypeGIDFromName(i.selectCol.name)._2, i.asName.name)).toMap

      val groupByOp = new GroupByOperator(tailOperator,keys,functions,this)
      val getGroupByNewNameFromNewGID = groupByOp.getNewGIDFromOldGID.map(tp => (tp._2 , getNewNameFromOldGID(tp._1))).toMap

      tailOperator = groupByOp
      val getNameTypeFromGID = getTypeGIDFromName.map(tp => (tp._2._2,(tp._1,tp._2._1))).toMap
      getTypeGIDFromName =  tailOperator.outputSchema.getSchemaArray.map(tp =>
        (
          if(getGroupByNewNameFromNewGID.contains(tp._2)){
            getGroupByNewNameFromNewGID(tp._2)
          }else{
            getNameTypeFromGID(tp._2)._1
          },
          (tp._1, tp._2)
          )).toMap
      println(getTypeGIDFromName)

    }
    if(sql.contains("join")){
      sql("join").asInstanceOf[List[JoinStatement]].foreach(j => {
        val rightTable = tables(j.table.name)
        if(j.joinType == "inner"){
          val leftParent = tailOperator
          val rightParent = rightTable.getSinkOperator
          val joinCond = j.onCol.map(tp => {
            if(joinedTables.contains(tp._1.table.name)
              && tp._2.table.name == j.table.name){
              ( if(getTypeGIDFromName.contains(tp._1.column.name))
                  getTypeGIDFromName(tp._1.column.name)._2
                else if(getTypeGIDFromName.contains(tp._1.table.name + "." + tp._1.column.name))
                  getTypeGIDFromName(tp._1.table.name + "." + tp._1.column.name)._2
                else
                  throw new Exception("Can't find  table name")
                ,
                rightTable.getTypeGIDFromName(tp._2.column.name)._2)
            } else if(joinedTables.contains(tp._2.table.name)
              && tp._1.table.name == j.table.name){
              (rightTable.getTypeGIDFromName(tp._2.column.name)._2,
                if(getTypeGIDFromName.contains(tp._1.column.name))
                  getTypeGIDFromName(tp._1.column.name)._2
                else if(getTypeGIDFromName.contains(tp._1.table.name + "." + tp._1.column.name))
                  getTypeGIDFromName(tp._1.table.name + "." + tp._1.column.name)._2
                else
                  throw new Exception("Can't find  table name")
                )
            }else{
              throw new Exception("join condition mismatch")
            }
          }).toIndexedSeq
          joinedTables += j.table.name -> tables(j.table.name)
          tailOperator = new InnerJoinOperator(leftParent,rightParent, joinCond, this)

          val getNameTypeFromGID = getTypeGIDFromName.map(tp => (tp._2._2,(tp._1,tp._2._1))).toMap
          val rightTable_getNameTypeFromGID = rightTable.getTypeGIDFromName.map(tp => (tp._2._2,(tp._1,tp._2._1))).toMap
          getTypeGIDFromName = tailOperator.outputSchema.getSchemaArray.map(tp => {
            (
            if(getNameTypeFromGID.contains(tp._2)){
              val newName = getNameTypeFromGID(tp._2)._1
              if(!newName.contains("."))
                sql("from").asInstanceOf[Identifier].name + "." + newName
              else
                newName
            }
            else if(rightTable_getNameTypeFromGID.contains(tp._2)){
              println("join " + j.table.name + "." + rightTable_getNameTypeFromGID(tp._2)._1 )
              j.table.name + "." + rightTable_getNameTypeFromGID(tp._2)._1
            }
            else
              throw new Exception("Can't find GID")
            ,
            (tp._1, tp._2))
          }).toMap
        }
      })
    }


    val getSelectItemFromGID = sql("select").asInstanceOf[List[SelectItem]].map(si => ({
      if(si.asName != null && getTypeGIDFromName.contains(si.asName.name))
        getTypeGIDFromName(si.asName.name)._2
      else if(si.table != null && si.selectCol != null && getTypeGIDFromName.contains(si.table.name + "." + si.selectCol.name))
        getTypeGIDFromName(si.table.name + "." + si.selectCol.name)._2
      else if(getTypeGIDFromName.contains(si.selectCol.name))
        getTypeGIDFromName(si.selectCol.name)._2
      else
        throw new Exception("cant find name " + si)
    },si)).toMap

    tailOperator = new SelectOperator(tailOperator,getSelectItemFromGID.keys.toIndexedSeq,this)

    getTypeGIDFromName = getTypeGIDFromName.filter(tp => getSelectItemFromGID.contains(tp._2._2)).map(tp => (
      {
        val si = getSelectItemFromGID(tp._2._2)
        if(si.asName != null)
          si.asName.name
        else
          si.selectCol.name
      }
      ,tp._2))


    tables += t.name -> new Table(getTypeGIDFromName, tailOperator)
  }





  def test(args : Array[String]){
    this.args = args
    new PrintWriter(new File("results/" + args(1)  + ".txt" ))
    val parsedLines = parser.parseFile("sql.txt")
    parsedLines.foreach(p => executeQuery(p) )



    print(this.operatorGraph.toString())

//    val f = (record : IndexedSeq[Any], schema : Schema) => {
//      record.head.asInstanceOf[Int] > 0
//    }
//
//    val f2 = (record : IndexedSeq[Any], schema : Schema) => {
//      record.tail.head.asInstanceOf[Int] > 0
//    }
//
//
//    val s = new Schema(IndexedSeq(
//      ("int", columns.getGlobalColId),
//      ("int", columns.getGlobalColId),
//      ("int", columns.getGlobalColId),
//      ("int", columns.getGlobalColId),
//      ("int", columns.getGlobalColId),
//      ("int", columns.getGlobalColId)) )

//    socketTextStream("localhost", 9999, "ii")
//    val in = new ParseOperator(s, "ii", this)
//    val wh = new WhereOperator(in,f,this)
//    val wh2 = new WhereOperator(wh,f2,this)
//    val ou = new OutputOperator(wh2, IndexedSeq(0,1,2,3), this)

//    val x = new ParseOperator(s, "ii", this)
//    val in = new WindowOperator(x, 5,this)


//    val se = new SelectOperator(in,IndexedSeq(0,1), this)
//    val wh = new WhereOperator(se,f,Set(0),this){selectivity = 0.9}
//    val se2 = new SelectOperator(in,IndexedSeq(2,3), this)
//
//    val jo = new InnerJoinOperator(wh,se2,IndexedSeq((0,2)),this)
//    val wh2 = new WhereOperator(jo, f2, Set(1), this){selectivity = 0.7}
//    val ou = new OutputOperator(wh2, IndexedSeq(0,1,2,3), this)

//    val se0 = new SelectOperator(in,IndexedSeq(0,1), this)
//    val se1 = new SelectOperator(in,IndexedSeq(2,3), this)
//    val se2 = new SelectOperator(in,IndexedSeq(4,5), this)
//
//    val ja = new InnerJoinOperator(se0,se1,IndexedSeq((0,2)),this){selectivity = 2.1}
//    val jb = new InnerJoinOperator(ja,se2,IndexedSeq((0,4)),this){selectivity = 1.9}
//
//    val ou = new OutputOperator(jb, IndexedSeq(0,1,2,3,4,5), this)

//    print(this.operatorGraph.toString())
//    println("pushing")
//    this.operatorGraph.pushAllPredicates
//    print(this.operatorGraph.toString())
//    println("grouging where")
//    this.operatorGraph.groupPredicate()
//    print(this.operatorGraph.toString())
//    println("optimizing where")
//    this.operatorGraph.whereOperatorSets.foreach(s => s.optimize())

//    println("grouping & optimizing inner join")
//    this.operatorGraph.groupInnerJoin()
//    this.operatorGraph.InnerJoinOperatorSets.foreach(s => s.optimize())
//    print(this.operatorGraph.toString())

    start()
  }

}

class Execution(time:Time, inputRdds :scala.collection.mutable.Map[String, RDD[String]]){
  def getInputRdds = inputRdds
  def getTime = time
}



class Schema (schemaArray : IndexedSeq[(String, Int)]) extends Serializable{
  //schamaArray is an array of (className, globalID)
  val getClassFromLocalColId = schemaArray.zipWithIndex.map(kvp => (kvp._2,kvp._1._1)).toMap
  val getLocalIdFromGlobalId = schemaArray.zipWithIndex.map(kvp => (kvp._1._2, kvp._2)).toMap
  val getClassFromGlobalId = schemaArray.map(tp => (tp._2,tp._1)).toMap
  val getGlobalIdSet = schemaArray.map(_._2).toSet
  def getSchemaArray = schemaArray
  override def toString = schemaArray.toString()
}

class Table(nameTypeGID : Map[String,(String, Int)], sinkOperator : Operator){
  def getTypeGIDFromName = nameTypeGID
  def getSinkOperator = sinkOperator
}

object SqlHelper{
  def printRDD(rdd : RDD[_]) {
    rdd.collect.foreach(record => record match {
      case record:IndexedSeq[Any] => println(record.toList)
      case record:Any => println(record)
    })
  }

  var results : PrintWriter = null

  def write(s : String) = {
    results.write(s)
    results.flush()
  }
  def writeln(s : String) = write(s + "\n")
}