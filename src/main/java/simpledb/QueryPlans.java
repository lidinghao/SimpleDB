package simpledb;

public class QueryPlans {

	public QueryPlans(){
	}

	//SELECT * FROM T1, T2 WHERE T1.column0 = T2.column0;
	public Operator queryOne(DbIterator t1, DbIterator t2) {
		// IMPLEMENT ME

		JoinPredicate pred = new JoinPredicate(0, Predicate.Op.EQUALS,0);
		Join join = new Join(pred,t1,t2);
        return join;
	}

	//SELECT * FROM T1, T2 WHERE T1. column0 > 1 AND T1.column1 = T2.column1;
	public Operator queryTwo(DbIterator t1, DbIterator t2) {
		// IMPLEMENT ME
        //filter T1. column0 > 1
        Predicate pred1 = new Predicate(0, Predicate.Op.GREATER_THAN, new IntField(1));
        Filter filter = new Filter(pred1,t1);

        // join
        JoinPredicate pred2 = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join join = new Join(pred2, filter, t2);


		return join;
	}

	//SELECT column0, MAX(column1) FROM T1 WHERE column2 > 1 GROUP BY column0;
	public Operator queryThree(DbIterator t1) {
		// IMPLEMENT ME

        Predicate pred1 = new Predicate(2, Predicate.Op.GREATER_THAN, new IntField(1));
        Filter filter = new Filter(pred1,t1);

        Aggregate agg = new Aggregate(filter, 1, 0, Aggregator.Op.MAX);
        return agg;
	}

	// SELECT ​​* FROM T1, T2
	// WHERE T1.column0 < (SELECT COUNT(*​​) FROM T3)
	// AND T2.column0 = (SELECT AVG(column0) FROM T3)
	// AND T1.column1 >= T2. column1
	// ORDER BY T1.column0 DESC;
	public Operator queryFour(DbIterator t1, DbIterator t2, DbIterator t3) throws TransactionAbortedException, DbException {
		// IMPLEMENT ME

		//(SELECT COUNT(*​​) FROM T3)
        Aggregate agg1 = new Aggregate(t3,0,-1, Aggregator.Op.COUNT);

		//T1.column0 < (SELECT COUNT(*​​) FROM T3)
		agg1.open();
		Field field  = agg1.fetchNext().getField(0);
		agg1.close();
		Predicate pred1= new Predicate(0, Predicate.Op.LESS_THAN,field);

		Filter filter1 = new Filter(pred1, t1);

		//(SELECT AVG(column0) FROM T3)
		Aggregate agg2 = new Aggregate(t3,0,-1, Aggregator.Op.AVG);

        //T2.column0 = (SELECT AVG(column0) FROM T3)
		agg2.open();
		Field field2  = agg2.fetchNext().getField(0);
		agg2.close();

		Predicate pred2= new Predicate(0, Predicate.Op.EQUALS,field2);

		Filter filter2 = new Filter(pred2, t2);

		//T1.column1 >= T2. column1
		JoinPredicate pred3 = new JoinPredicate(1, Predicate.Op.GREATER_THAN_OR_EQ,1);
		Join join = new Join(pred3,filter1, filter2);

		//ORDER BY T1.column0 DESC
		OrderBy orderBy = new OrderBy(0, false, join);
		return orderBy;
	}


}