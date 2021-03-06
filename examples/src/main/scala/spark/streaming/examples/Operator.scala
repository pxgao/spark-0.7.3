package spark.streaming.examples

import spark.streaming._
import spark.RDD
import scala.collection.mutable
import spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import scala.actors.Actor._
import scala.actors.Actor
import spark.Accumulator
/**
 * Created with IntelliJ IDEA.
 * User: peter
 * Date: 10/13/13
 * Time: 9:43 PM
 * To change this template use File | Settings | File Templates.
 */
abstract class Operator {
  var sqlContext : SqlSparkStreamingContext = null
  var parentOperators = mutable.ArrayBuffer[Operator]()
  var childOperators = mutable.ArrayBuffer[Operator]()
  var outputSchema : Schema = null

  def getChildOperators = childOperators
  def execute(exec : Execution) : Array[RDD[IndexedSeq[Any]]]
  def replaceParent(oldParent : Operator, newParent : Operator)
  override def toString = this.getClass.getSimpleName + "@" + this.hashCode() + " "
  def toStringWithIndent(offset : Int) : String = {
    def genIndent(offset : Int) : StringBuilder = offset match{
      case 0 => new mutable.StringBuilder()
      case offset => genIndent(offset-1).append(" ")
    }
    genIndent(offset) + toString
  }

  def getFamilyTree(generation : Int) : String = {
    val str = new mutable.StringBuilder()

    def addRecord(op : Operator, offset:Int){
      str.append(op.toStringWithIndent(offset) + "\n")
      op.parentOperators.foreach(parent => addRecord(parent, offset + 2))
    }
    addRecord(this,0)
    str.toString()
  }
}

class InnerJoinOperatorSet(parentCtx : SqlSparkStreamingContext) extends Operator {
  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)
  var tailOperator: Operator = null
  def InnerJoinOperatorOrdering = new Ordering[InnerJoinOperator]{
    def compare(a : InnerJoinOperator, b : InnerJoinOperator) = b.selectivity.compare(a.selectivity)
  }
  val innerJoinOperators = mutable.PriorityQueue[InnerJoinOperator]()(InnerJoinOperatorOrdering)

  def addInnerJoinOperator(op : InnerJoinOperator){
    innerJoinOperators += op
    if(innerJoinOperators.size == 1){
      outputSchema = op.outputSchema
      tailOperator = op
    }
  }

  def addParent(op : Operator){
    parentOperators += op
  }

  def optimize(){
    def build(remainingParents : mutable.Set[Operator], remainingOperators : mutable.PriorityQueue[InnerJoinOperator]) : mutable.Set[Operator] = {
      if(remainingOperators.size == 0){
        return remainingParents
      }else{
        val op = remainingOperators.dequeue()
        println("dequeue:" + op)
        val leftParent = remainingParents.filter(p => op.leftJoinSet.subsetOf(p.outputSchema.getGlobalIdSet)).head
        val rightParent = remainingParents.filter(p => op.rightJoinSet.subsetOf(p.outputSchema.getGlobalIdSet) && p != leftParent).head
        op.setParents(leftParent, rightParent)
        remainingParents -= leftParent
        remainingParents -= rightParent
        remainingParents += op
        build(remainingParents, remainingOperators)
      }
    }

    val remainingParents = mutable.Set[Operator]()
    parentOperators.foreach(remainingParents += _)
    tailOperator = build(remainingParents, innerJoinOperators.clone()).head
    outputSchema = tailOperator.outputSchema
  }



  override def execute(exec : Execution) : Array[RDD[IndexedSeq[Any]]] = {
    tailOperator.execute(exec)
  }

  def replaceParent(oldParent: Operator, newParent: Operator) {
    parentOperators -= oldParent
    parentOperators += newParent
  }

  override def toString = {
    def toStrRecursive(op : Operator, depth : Int = 0) : String = {
      if(innerJoinOperators.exists(_ == op))
        depth + ":" + op.toString + "\n" + op.parentOperators.map(p => toStrRecursive(p, depth + 1)).reduce(_ + "\n" + _)
      else
        ""
    }
    super.toString + "\n" + toStrRecursive(tailOperator)
  }
}



class WhereOperatorSet(parentCtx : SqlSparkStreamingContext) extends UnaryOperator{
  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)
  val operators = mutable.PriorityQueue[WhereOperator]()(WhereOperatorOrdering)
  var tailOperator: Operator = null

  def WhereOperatorOrdering = new Ordering[WhereOperator]{
    def compare(a : WhereOperator, b : WhereOperator) = b.selectivity.compare(a.selectivity)
  }

  def addWhereOperator(op : WhereOperator){
    operators += op
    if(operators.size == 1){
      outputSchema = op.outputSchema
    }
  }

  def optimize(){
    def built(parent : Operator, remaining :  mutable.PriorityQueue[WhereOperator]) : Operator = {
      if(remaining.size == 0)
        parent
      else{
        val current = remaining.dequeue()
        current.setParent(parent)
        built(current, remaining)
      }
    }

    tailOperator = built(parentOperators.head, operators.clone())
  }

  override def execute(exec : Execution) : Array[RDD[IndexedSeq[Any]]] = {
    tailOperator.execute(exec)
  }

  override def toString = {
    def toStrRecursive(op : Operator) : String = {
      if(operators.exists(_ == op))
        op.toString + "\n" + toStrRecursive(op.parentOperators.head)
      else
        ""
    }
    super.toString + "\n" + toStrRecursive(tailOperator)
  }

}

abstract class UnaryOperator extends Operator{
  def setParent(parentOp : Operator) {
    if(parentOperators.size > 0){
      parentOperators.head.childOperators -= this
      parentOperators.clear()
    }
    parentOperators += parentOp
    parentOp.childOperators += this
  }

  override def replaceParent(oldParent : Operator, newParent : Operator){
    setParent(newParent)
  }
}

abstract class BinaryOperator extends Operator{
  def setParents(parentOp1 : Operator, parentOp2 : Operator){
    if(parentOperators.size > 0){
      parentOperators.foreach(p => p.childOperators -= this)
      parentOperators.clear()
    }
    parentOperators += parentOp1
    parentOperators += parentOp2
    parentOp1.childOperators += this
    parentOp2.childOperators += this
  }

  override def replaceParent(oldParent : Operator, newParent : Operator){
    if(parentOperators(0) == oldParent)
      setParents(newParent, parentOperators(1))
    else if(parentOperators(1) == oldParent)
      setParents(parentOperators(0), newParent)
    else
      throw new Exception("The original parent does not exist")
  }
}

class WindowOperator(parentOp : Operator, batches : Int, parentCtx : SqlSparkStreamingContext) extends UnaryOperator{
  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)
  setParent(parentOp)
  var cached = Map[Time,Array[RDD[IndexedSeq[Any]]]]()

  override def setParent(parentOp : Operator){
    super.setParent(parentOp)
    outputSchema = parentOp.outputSchema
  }

  override def execute(exec : Execution) : Array[RDD[IndexedSeq[Any]]] = {
    val resultFromParent = parentOperators.head.execute(exec)
    cached += exec.getTime -> resultFromParent
    cached = cached.filter(kvp => kvp._1 > exec.getTime - this.parentCtx.getBatchDuration * batches)
    cached.foreach(tp => tp._2.foreach(_.persist()))
    cached.flatMap(tp => tp._2).toArray
  }

}


class SelectOperator(parentOp : Operator, selectColGlobalId : IndexedSeq[Int], parentCtx : SqlSparkStreamingContext) extends UnaryOperator {
  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)
  var isSelectAll = false
  var localColId = IndexedSeq[Int]()
  setParent(parentOp)
  outputSchema = new Schema(selectColGlobalId.map(gid => (parentOperators.head.outputSchema.getClassFromGlobalId(gid),gid)))

  var cached = Map[RDD[IndexedSeq[Any]], RDD[IndexedSeq[Any]]]()

  def getSelectColGlobalId = selectColGlobalId

  override def setParent(parentOp : Operator){
    super.setParent(parentOp)
    isSelectAll = selectColGlobalId.toSeq.equals(parentOperators.head.outputSchema.getLocalIdFromGlobalId.keySet)
    localColId = selectColGlobalId.map(gid => parentOp.outputSchema.getLocalIdFromGlobalId(gid))
  }

  override def execute(exec : Execution) : Array[RDD[IndexedSeq[Any]]] = {
    cached = parentOperators.head.execute(exec).map(rdd => (rdd,{
      if(cached.contains(rdd))
        cached(rdd)
      else
        if(isSelectAll)
          rdd
        else{
          val localColId = this.localColId
          //println("print in select op")
          //SqlHelper.printRDD(rdd)
          rdd.map(record => localColId.map(id => record(id)) )
        }
    })).toMap;
    cached.values.toArray
  }

  override def toString = super.toString + selectColGlobalId
}

class WhereOperator(parentOp : Operator, func : (IndexedSeq[Any], Schema) => Boolean, whereColumnId : Set[Int], parentCtx : SqlSparkStreamingContext) extends UnaryOperator{
  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)
  setParent(parentOp)


  var cached = Map[RDD[IndexedSeq[Any]], RDD[IndexedSeq[Any]]]()

  var selectivity = 1.0

  def getWhereColumnId = whereColumnId

  override def setParent(parentOp : Operator){
    super.setParent(parentOp)
    outputSchema = parentOp.outputSchema
  }

  override def execute(exec : Execution) : Array[RDD[IndexedSeq[Any]]] = {
    val func = this.func
    val outputSchema = this.outputSchema

    cached = parentOperators.head.execute(exec).map(rdd => (rdd, {
      if(cached.contains(rdd))
        cached(rdd)
      else
        rdd.filter(record => func(record, outputSchema))
    })).toMap
    cached.values.toArray
  }

  override def toString = super.toString + whereColumnId + "Sel:" + selectivity
}

class GroupByCombiner(createCombiner : Any, mergeValue : Any, mergeCombiners : Any, finalProcessing : Any, resultType : String) extends Serializable{
  def getCreateCombiner = createCombiner.asInstanceOf[Any=>Any]
  def getMergeValue = mergeValue.asInstanceOf[(Any,Any)=>Any]
  def getMergeCombiners = mergeCombiners.asInstanceOf[(Any,Any)=>Any]
  def getFinalProcessing = finalProcessing.asInstanceOf[Any=>Any]
  def getResultType = resultType
}

class GroupByOperator(parentOp : Operator, keyColumnsArr : IndexedSeq[Int], functions : Map[Int, GroupByCombiner], parentCtx : SqlSparkStreamingContext) extends UnaryOperator{
  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)

  val valueColumns = functions.keySet
  val keyColumns = keyColumnsArr.toSet
  assert(keyColumns.intersect(valueColumns).isEmpty)
  setParent(parentOp)
  val getOldGIDFromNewGID = functions.map(tp => (sqlContext.columns.getGlobalColId, tp._1)).toMap
  val getNewGIDFromOldGID = getOldGIDFromNewGID.map(tp => (tp._2,tp._1)).toMap

  outputSchema = new Schema(keyColumnsArr.map(gid => (parentOp.outputSchema.getClassFromGlobalId(gid),gid))
    ++ functions.map(kvp => (kvp._2.getResultType, getNewGIDFromOldGID(kvp._1))))

  var cached = Map[RDD[IndexedSeq[Any]],RDD[(IndexedSeq[Any],IndexedSeq[Any])]]()


  def getKeyColumnsArr = keyColumnsArr
  def getFunctions = functions

  override def setParent(parentOp : Operator){
    super.setParent(parentOp)
    assert(keyColumns.union(valueColumns).subsetOf(parentOp.outputSchema.getSchemaArray.map(_._2).toSet))
  }

  override def execute(exec : Execution) : Array[RDD[IndexedSeq[Any]]] = {
    val rddPair = parentOperators.head.execute(exec).map(rdd => (rdd, {
      if(this.parentCtx.args.contains("-incre") && this.parentCtx.incrementalOperator && cached.contains(rdd)){
        cached(rdd)
      }
      else
        groupBy(rdd)
    })).toMap
    val unioned = this.parentCtx.ssc.sc.union[(IndexedSeq[Any],IndexedSeq[Any])](rddPair.values.toSeq)
    val result = Array[RDD[IndexedSeq[Any]]](mergeBatch(unioned))
    cached = rddPair
    cached.foreach(kvp => kvp._2.persist)
    result
  }

  //TODO: Try to keep the partition info by avoiding map
  def groupBy(rdd : RDD[IndexedSeq[Any]]) : RDD[(IndexedSeq[Any],IndexedSeq[Any])] = {
    val createCombiner = (x : IndexedSeq[Any], func : Map[Int, GroupByCombiner]) => {
      func.map(kvp => kvp._2.getCreateCombiner(x(kvp._1))).toIndexedSeq
    }

    val mergeValue = (x : IndexedSeq[Any], y : IndexedSeq[Any], func : Map[Int, GroupByCombiner]) => {
      func.map(kvp => kvp._2.getMergeValue(x(kvp._1), y(kvp._1))).toIndexedSeq
    }

    val mergeCombiners = (x : IndexedSeq[Any], y : IndexedSeq[Any], func : Map[Int, GroupByCombiner]) => {
      func.map(kvp => kvp._2.getMergeCombiners(x(kvp._1), y(kvp._1))).toIndexedSeq
    }


    val localKeyColumnArr = this.keyColumnsArr.map(parentOp.outputSchema.getLocalIdFromGlobalId(_))
    val localFunctions = this.functions.map(kvp => (parentOp.outputSchema.getLocalIdFromGlobalId(kvp._1), kvp._2) )
    val localValueFunctions = localFunctions.zipWithIndex.map(kvp => (kvp._2,kvp._1._2))//the location in the groupby columns

    val kvpRdd = rdd.map(record => (
      localKeyColumnArr.map(record(_)).toIndexedSeq,
      localFunctions.map(kvp => record(kvp._1)).toIndexedSeq)
    )


    kvpRdd.combineByKey[IndexedSeq[Any]](
      (x : IndexedSeq[Any]) => createCombiner(x,localValueFunctions),
      (x : IndexedSeq[Any],y : IndexedSeq[Any])=>mergeValue(x,y,localValueFunctions),
      (x : IndexedSeq[Any],y : IndexedSeq[Any])=>mergeCombiners(x,y,localValueFunctions)
    ).map(kvp => (kvp._1,kvp._2))
//    val reduced = combined.mapValues(finalProcessing(_,localValueFunctions))
//    val rr = reduced.map(kvp => kvp._1 ++ kvp._2)
//    rr.map(arr => arr.toIndexedSeq)
  }

  def mergeBatch(rdd : RDD[(IndexedSeq[Any],IndexedSeq[Any])]) : RDD[IndexedSeq[Any]] = {

    val mergeCombiners = (x : IndexedSeq[Any], y : IndexedSeq[Any], func : Map[Int, GroupByCombiner]) => {
      func.map(kvp => kvp._2.getMergeCombiners(x(kvp._1), y(kvp._1))).toIndexedSeq
    }

    val finalProcessing = (x : IndexedSeq[Any], func : Map[Int, GroupByCombiner]) => {
      func.map(kvp => kvp._2.getFinalProcessing(x(kvp._1))).toIndexedSeq
    }

    val localFunctions = this.functions.map(kvp => (parentOp.outputSchema.getLocalIdFromGlobalId(kvp._1), kvp._2) )
    val localValueFunctions = localFunctions.zipWithIndex.map(kvp => (kvp._2,kvp._1._2))//the location in the groupby columns

    rdd.reduceByKey((x,y) => mergeCombiners(x,y,localValueFunctions)).map(kvp => kvp._1 ++ finalProcessing(kvp._2,localValueFunctions))

  }

  override def toString = super.toString + "keys:" + keyColumnsArr + " values:" + functions
}




class ParseOperator(schema : Schema, delimiter : String, inputStreamName : String, parentCtx : SqlSparkStreamingContext) extends UnaryOperator {
  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)
  outputSchema = schema

  override def execute(exec : Execution) : Array[RDD[IndexedSeq[Any]]] = {

    val outputSchema = this.outputSchema
    val delimiter = this.delimiter
    val parseLine =  (line : String) => {
      val parse = (str : String, tp : String) => {
        tp match{
          case "int" => str.toInt
          case "double" => str.toDouble
          case "string" => str
          case _ => throw new Exception("unknown data type")
        }
      }

      val lineArr = line.trim.split(delimiter).toIndexedSeq

      if (lineArr.length != outputSchema.getSchemaArray.length){
        IndexedSeq[Any]()
      }
      else
        try{
          lineArr.zipWithIndex.map(entry => parse(entry._1, outputSchema.getSchemaArray(entry._2)._1 ))
        }catch{
          case e : Exception => IndexedSeq[Any]()
        }
    }

    val rdd = exec.getInputRdds(inputStreamName)
    val returnRDD = rdd.map(line => parseLine(line)).filter(line => line.length > 0)
    Array(returnRDD)
  }

  override def setParent(parentOp : Operator){}

  override def toString = super.toString + schema
}


class OutputOperator(parentOp : Operator, selectColGlobalId : IndexedSeq[Int], parentCtx : SqlSparkStreamingContext) extends UnaryOperator {
  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)
  var isSelectAll = false
  var localColId = IndexedSeq[Int]()
  setParent(parentOp)
  outputSchema = new Schema(selectColGlobalId.map(gid => (parentOperators.head.outputSchema.getClassFromGlobalId(gid),gid)))

  override def setParent(parentOp : Operator){

    super.setParent(parentOp)
    isSelectAll = selectColGlobalId.toSeq.equals(parentOperators.head.outputSchema.getLocalIdFromGlobalId.keySet)
    localColId = selectColGlobalId.map(gid => parentOp.outputSchema.getLocalIdFromGlobalId(gid))
  }

  override def execute(exec : Execution) : Array[RDD[IndexedSeq[Any]]] = {
    val rdd = this.parentCtx.ssc.sc.union[IndexedSeq[Any]]( parentOperators.head.execute(exec).toSeq)

    val returnRDD =
      if(isSelectAll)
        rdd
      else{
        val localColId = this.localColId
        //println("print in select op")
        //SqlHelper.printRDD(rdd)
        rdd.map(record => localColId.map(id => record(id)) )
      }
    Array(returnRDD)
  }
}


class InnerJoinOperator(parentOp1 : Operator, parentOp2 : Operator, joinCondition : IndexedSeq[(Int, Int)], parentCtx : SqlSparkStreamingContext) extends BinaryOperator {
  sqlContext = parentCtx
  sqlContext.operatorGraph.addOperator(this)
  var getLocalIdFromGlobalId = Map[Int,Int]()
  setParents(parentOp1, parentOp2)
  val leftJoinSet = joinCondition.map(tp=>tp._1).toSet
  val rightJoinSet = joinCondition.map(tp=>tp._2).toSet

  var cached = Map[(RDD[IndexedSeq[Any]], RDD[IndexedSeq[Any]]), RDD[IndexedSeq[Any]]]()

  var selectivity : Double = 1.0

  override def setParents(parentOp1 : Operator, parentOp2 : Operator){
    super.setParents(parentOp1, parentOp2)
    joinCondition.foreach(tp => assert(parentOp1.outputSchema.getClassFromGlobalId(tp._1) == parentOp2.outputSchema.getClassFromGlobalId(tp._2)))
    outputSchema = new Schema(parentOp1.outputSchema.getSchemaArray.toSet.union(parentOp2.outputSchema.getSchemaArray.toSet).toArray.sortBy(_._2))
    val left = parentOp1.outputSchema.getLocalIdFromGlobalId
    val right = parentOp2.outputSchema.getLocalIdFromGlobalId.map(kvp => (kvp._1, kvp._2 + left.size))
    getLocalIdFromGlobalId = left ++ right
  }


  override def execute(exec : Execution) : Array[RDD[IndexedSeq[Any]]] = {
    val leftParentResult = parentOperators(0).execute(exec)
    val rightParentResult = parentOperators(1).execute(exec)
    var result = Map[(RDD[IndexedSeq[Any]], RDD[IndexedSeq[Any]]), RDD[IndexedSeq[Any]]]()
    for(leftRdd <- leftParentResult ; rightRdd <- rightParentResult){
      val res :  RDD[IndexedSeq[Any]] =
        if(this.parentCtx.args.contains("-incre") && cached.contains((leftRdd, rightRdd))){
          cached((leftRdd, rightRdd))
        }
        else
          join(leftRdd, rightRdd)
      result += (leftRdd, rightRdd) -> res
    }
    result.foreach(tp => tp._2.persist())
    cached = result
    result.values.toArray
  }


  def join(left : RDD[IndexedSeq[Any]], right : RDD[IndexedSeq[Any]]) = {
    val localJoinCondition = joinCondition.map(tp => (parentOperators(0).outputSchema.getLocalIdFromGlobalId(tp._1), parentOperators(1).outputSchema.getLocalIdFromGlobalId(tp._2)))
    val getLocalIdFromGlobalId = this.getLocalIdFromGlobalId
    val outputSchema = this.outputSchema


    val rdd1 = left.map(record => (localJoinCondition.map(tp => record(tp._1)),record))
    val rdd2 = right.map(record => (localJoinCondition.map(tp => record(tp._2)),record))


    //use a fully random partitioner, and then map partition
    val joined = rdd1.join(rdd2)
    val result = joined.map(pair => {
      val combined = pair._2._1 ++ pair._2._2
      outputSchema.getSchemaArray.map(kvp => combined(getLocalIdFromGlobalId(kvp._2)))
    }
    )


    if(this.parentCtx.args.contains("-reorder")){
      val joinAcc = parentCtx.ssc.sc.accumulator(0L)
      val rdd1Acc = parentCtx.ssc.sc.accumulator(0L)
      val rdd2Acc = parentCtx.ssc.sc.accumulator(0L)

      joined.foreach(l => joinAcc += 1)
      rdd1.foreach(l => rdd1Acc += 1)
      rdd2.foreach(l => rdd2Acc += 1)

      getSelectivityActor ! (rdd1Acc,rdd2Acc, joinAcc)
    }


    result
  }


  val getSelectivityActor = actor{
    while(true){
      try{
        receive{
          case (rdd1Acc : Accumulator[Long], rdd2Acc :Accumulator[Long], joinAcc : Accumulator[Long]) =>
          {
            val joinedSize = joinAcc.value
            val rdd1Size = rdd1Acc.value
            val rdd2Size = rdd2Acc.value
            if(rdd1Size > 0 && rdd2Size > 0)
            {
              selectivity = joinedSize.toDouble /(rdd1Size * rdd2Size)
            }
          }
        }
      }catch{
        case e : Exception => e.printStackTrace()
      }
    }
  }

  def getJoinCondition = joinCondition
  override def toString = super.toString + joinCondition + " Sel:" + selectivity
}