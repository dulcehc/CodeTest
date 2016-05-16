package com.linio.exam.main;

import java.io.File;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.linio.exam.labels.LABELS;
import com.linio.exam.relations.Relationships;

public class SQLToNode {
	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);
		/* SQL Pattern
		 * m.group(2): fields, m.group(4):from, fieldOp:m.group(6),
		 * operator:m.group(7), constant:m.group(8);
		 */
		String SQLPATTERN = "(SELECT|select)\\s(.+)\\s(FROM|from)\\s(.+)\\s(WHERE|where)\\s(\\w*)\\s?([<>!=]+)\\s?(\\w*)";

		String DBPATH = "C:/Users/Dulce/Documents/Neo4j/CodeTestDB";
		String sqlStm = "";

		System.out.print("SQL query: ");
		sqlStm = sc.nextLine();

		GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
		GraphDatabaseService db = dbFactory.newEmbeddedDatabase(new File(DBPATH));

		try (Transaction tx = db.beginTx()) {
			Pattern p = Pattern.compile(SQLPATTERN);
			Matcher m = p.matcher(sqlStm);

			if (m.find()) {

				// Node SELECT
				Node selectNode = db.createNode(LABELS.SELECT);
				selectNode.setProperty("name", "SELECT");
				selectNode.setProperty("from", m.group(4));

				// Nodes fields 
				for (String field : m.group(2).split("\\s?,\\s?")) {
					Node f = db.createNode(LABELS.FIELD);
					f.setProperty("name", field);
					selectNode.createRelationshipTo(f, Relationships.SELECTS);
				}

				// Node WHERE
				Node whereNode = db.createNode(LABELS.WHERE);
				whereNode.setProperty("name", "WHERE");

				// Node operator
				Node operatorNode = db.createNode(LABELS.OPERATOR);
				operatorNode.setProperty("name", m.group(7));

				// Node constant
				Node consNode = db.createNode(LABELS.CONSTANT);
				consNode.setProperty("name", m.group(8));

				// Find the node field for the operation 
				// OPERATOR -> FIELD 
				ResourceIterator<Node> nodesFields = db.findNodes(LABELS.FIELD);
				while (nodesFields.hasNext()) {
					Node nodField = nodesFields.next();
					if (nodField.getProperty("name").equals(m.group(6))) {
						operatorNode.createRelationshipTo(nodField, Relationships.OPERANDS);
						break;
					}
				}

				// WHERE -> OPERATOR
				whereNode.createRelationshipTo(operatorNode, Relationships.RULE);
				// WHERE -> SELECT
				whereNode.createRelationshipTo(selectNode, Relationships.FILTERS);
				// OPERATOR -> CONSTANT
				operatorNode.createRelationshipTo(consNode, Relationships.OPERANDS);

				//print relationships
				printRelationships(selectNode, Relationships.SELECTS);
				printRelationships(whereNode, Relationships.FILTERS);
				printRelationships(whereNode, Relationships.RULE);
				printRelationships(operatorNode, Relationships.OPERANDS);
				tx.success();

			} else {
				System.out.println("Invalid SQL Statement");
			}
		}
		System.out.println("...Done");
	}

	static void printRelationships(Node n, Relationships r) {
		System.out.print("["+n.getProperty("name")+"]"+" -> ["+r+"] -> ");
		for (Relationship relationship : n.getRelationships(r)) {
			Node node = relationship.getOtherNode(n);
			System.out.print("\t" + node.getProperty("name"));
		}
		System.out.println();
	}
}
