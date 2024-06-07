package sqlancer.sqlite3.gen.dml;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Constant;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;
import sqlancer.sqlite3.ast.SQLite3Expression;

public final class SQLite3DeleteGenerator {

    private final SQLite3GlobalState globalState;
    private final Randomly r;
    private final boolean atTopLevel;

    public SQLite3DeleteGenerator(SQLite3GlobalState globalState, Randomly r) {
        this(globalState, r, false);
    }

    public SQLite3DeleteGenerator(SQLite3GlobalState globalState, Randomly r, boolean atTopLevel) {
        this.globalState = globalState;
        this.r = r;
        this.atTopLevel = atTopLevel;
    }

    public static SQLQueryAdapter deleteContent(SQLite3GlobalState globalState) {
        try {
            SQLite3Table tableName = globalState.getSchema().getRandomTable(t -> !t.isView() && !t.isReadOnly());
            return deleteContent(globalState, tableName);
        } catch (Exception e) {
            ExpectedErrors errors = new ExpectedErrors();
            errors.add("[SQLITE_ERROR] SQL error or missing database (no such table: none)");
            return new SQLQueryAdapter("DELETE FROM none", errors, true);
        }
    }

    public static SQLQueryAdapter deleteContentAtTopLevel(SQLite3GlobalState globalState) {
        try {
            SQLite3Table tableName = globalState.getSchema().getRandomTable(t -> !t.isView() && !t.isReadOnly());
            return deleteContent(globalState, tableName, true);
        } catch (Exception e) {
            ExpectedErrors errors = new ExpectedErrors();
            errors.add("[SQLITE_ERROR] SQL error or missing database (no such table: none)");
            return new SQLQueryAdapter("DELETE FROM none", errors, true);
        }
    }

    public static SQLQueryAdapter deleteContent(SQLite3GlobalState globalState, SQLite3Table tableName) {
        return deleteContent(globalState, tableName, false);
    }

    public static SQLQueryAdapter deleteContent(SQLite3GlobalState globalState, SQLite3Table tableName, boolean atTopLevel) {
        SQLite3DeleteGenerator generator = new SQLite3DeleteGenerator(globalState, globalState.getRandomly(), atTopLevel);
        return generator.generate(tableName);
    }

    private SQLQueryAdapter generate(SQLite3Table tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ");
        sb.append(tableName.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(SQLite3Visitor.asString(new SQLite3ExpressionGenerator(globalState)
                    .setColumns(tableName.getColumns()).generateExpression()));
        }
        if (globalState.getDbmsSpecificOptions().testUpdateDeleteLimit && atTopLevel) {
            if (Randomly.getBooleanWithRatherLowProbability()) {
                // ORDER BY
                List<SQLite3Expression> expressions = new SQLite3ExpressionGenerator(globalState).setColumns(tableName.getColumns()).generateOrderBys();
                if (!expressions.isEmpty()) {
                    sb.append(" ORDER BY ");
                    sb.append(expressions.stream().map(e -> SQLite3Visitor.asString(e)).collect(Collectors.joining(", ")));
                }
            }

            if (Randomly.getBooleanWithRatherLowProbability()) {
                // LIMIT
                sb.append(" LIMIT ");
                sb.append(SQLite3Visitor.asString(SQLite3Constant.createIntConstant(r.getInteger())));
                if (Randomly.getBoolean()) {
                    // OFFSET
                    sb.append(" OFFSET ");
                    sb.append(SQLite3Visitor.asString(SQLite3Constant.createIntConstant(r.getInteger())));
                }
            }
        }

        ExpectedErrors errors = new ExpectedErrors();
        SQLite3Errors.addExpectedExpressionErrors(errors);
        errors.addAll(Arrays.asList("[SQLITE_ERROR] SQL error or missing database (foreign key mismatch",
                "[SQLITE_CONSTRAINT]  Abort due to constraint violation ",
                "[SQLITE_ERROR] SQL error or missing database (parser stack overflow)",
                "[SQLITE_ERROR] SQL error or missing database (no such table:", "no such column",
                "too many levels of trigger recursion", "cannot UPDATE generated column",
                "cannot INSERT into generated column", "A table in the database is locked",
                "load_extension() prohibited in triggers and views", "The database file is locked"));
        SQLite3Errors.addDeleteErrors(errors);


        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
