package hw1;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class Query {

	private String q;

	public Query(String q) {
		this.q = q;
	}

	public Relation execute() {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect) selectStatement.getSelectBody();
		
		// your code here
//		return null;
		ColumnVisitor colVisitor = new ColumnVisitor();
		Catalog catalog = Database.getCatalog();
		TablesNamesFinder tableNameFinder = new TablesNamesFinder();
		List<String> tableList = tableNameFinder.getTableList(statement);
		int tablebId = catalog.getTableId(tableList.get(0));
		ArrayList<Tuple> tupleList = catalog.getDbFile(tablebId).getAllTuples();
		TupleDesc td = catalog.getTupleDesc(tablebId);
		Relation curRelation = new Relation(tupleList, td);

		// JOIN clause
		Relation joinRelation = curRelation;
		List<Join> joinList = sb.getJoins();

		if (joinList != null) {
			for (Join join : joinList) {
				String tableName = join.getRightItem().toString();
				TupleDesc joinTupleDesc = catalog.getTupleDesc(catalog.getTableId(tableName));
				ArrayList<Tuple> joinTupleList = catalog.getDbFile(catalog.getTableId(tableName)).getAllTuples();
				Relation newRelation = new Relation(joinTupleList, joinTupleDesc);
				String[] splitJoin = join.getOnExpression().toString().split("=");
				String[] left = splitJoin[0].trim().split("\\.");
				String[] right = splitJoin[1].trim().split("\\.");
//				String table1 = left[0];
				String field1 = left[1];
//				String table2 = right[0];
				String field2 = right[1];
				
				int index1 = curRelation.getDesc().nameToId(field1);
				int index2 = newRelation.getDesc().nameToId(field2);

				joinRelation = joinRelation.join(newRelation, index1, index2);
			}
		}

		// WHERE clause
		Relation whereRelation = joinRelation;
		WhereExpressionVisitor whereVisitor = new WhereExpressionVisitor();
		if (sb.getWhere() != null) {
			sb.getWhere().accept(whereVisitor);
			whereRelation = joinRelation.select(joinRelation.getDesc().nameToId(whereVisitor.getLeft()),
					whereVisitor.getOp(), whereVisitor.getRight());
		}

		// SELECT clause
		Relation selectRelation = whereRelation;
		List<SelectItem> selectList = sb.getSelectItems();
		ArrayList<Integer> fieldList = new ArrayList<Integer>();

		for (SelectItem item : selectList) {
			item.accept(colVisitor);
			String colSelect;
			if(colVisitor.isAggregate() == true) {
				colSelect = colVisitor.getColumn();
			} else {
				colSelect = item.toString();
			}
			if (colSelect.equals("*")) {
				for (int i = 0; i < whereRelation.getDesc().numFields(); i++) {
					fieldList.add(i);
				}
				break;
			}
			int field;
			if(colSelect.equals("*") && colVisitor.isAggregate() == true) {
				field = 0;
			} else {
				field = whereRelation.getDesc().nameToId(colSelect);
			}
			if (!fieldList.contains(field))
				fieldList.add(field);
		}
		selectRelation = whereRelation.project(fieldList);

		// Aggregation
		boolean groupBy = sb.getGroupByColumnReferences() != null;
		Relation aggregator;
		if(colVisitor.isAggregate() == true) {
			aggregator = selectRelation.aggregate(colVisitor.getOp(), groupBy);
		}
		else {
			aggregator = selectRelation;
		}
		return aggregator;

	}
}