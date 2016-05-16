package com.linio.exam.relations;
import org.neo4j.graphdb.RelationshipType;

public enum Relationships implements RelationshipType{
	FILTERS, OPERANDS, RULE, SELECTS;
}
