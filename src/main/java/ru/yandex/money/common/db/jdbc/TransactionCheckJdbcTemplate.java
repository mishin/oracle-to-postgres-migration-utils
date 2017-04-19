package ru.yandex.money.common.db.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.List;

/**
 * JdbcTemplate с проверкой наличия явно открытой транзакции при выполнении операций update, insert, delete
 * Created by vsozykin on 11.18.16.
 */
public class TransactionCheckJdbcTemplate extends JdbcTemplate {

    private static final Logger log = LoggerFactory.getLogger(TransactionCheckJdbcTemplate.class);

    public TransactionCheckJdbcTemplate() {
    }

    public TransactionCheckJdbcTemplate(DataSource dataSource) {
        super(dataSource);
    }

    public TransactionCheckJdbcTemplate(DataSource dataSource, boolean lazyInit) {
        super(dataSource, lazyInit);
    }

    @Override
    public int update(String sql, PreparedStatementSetter pss) {
        requireOpenTransaction(sql);
        return super.update(sql, pss);
    }

    @Override
    public int update(String sql, Object[] args, int[] argTypes) {
        requireOpenTransaction(sql);
        return super.update(sql, args, argTypes);
    }

    @Override
    public int update(String sql, Object... args) {
        requireOpenTransaction(sql);
        return super.update(sql, args);
    }

    @Override
    public int update(String sql) {
        requireOpenTransaction(sql);
        return super.update(sql);
    }

    @Override
    public int[] batchUpdate(String... sql) {
        requireOpenTransaction(sql[0]);
        return super.batchUpdate(sql);
    }

    @Override
    public int[] batchUpdate(String sql, BatchPreparedStatementSetter pss) {
        requireOpenTransaction(sql);
        return super.batchUpdate(sql, pss);
    }

    @Override
    public int[] batchUpdate(String sql, List<Object[]> batchArgs) {
        requireOpenTransaction(sql);
        return super.batchUpdate(sql, batchArgs);
    }

    @Override
    public int[] batchUpdate(String sql, List<Object[]> batchArgs, int[] argTypes) {
        requireOpenTransaction(sql);
        return super.batchUpdate(sql, batchArgs, argTypes);
    }

    @Override
    public <T> int[][] batchUpdate(String sql, Collection<T> batchArgs, int batchSize, ParameterizedPreparedStatementSetter<T> pss) {
        requireOpenTransaction(sql);
        return super.batchUpdate(sql, batchArgs, batchSize, pss);
    }

    @Override
    public void execute(String sql) {
        if (!isSelectSql(sql)) {
            requireOpenTransaction(sql);
        }
        super.execute(sql);
    }

    @Override
    public <T> T execute(String sql, PreparedStatementCallback<T> action) {
        if (!isSelectSql(sql)) {
            requireOpenTransaction(sql);
        }
        return super.execute(sql, action);
    }

    private static void requireOpenTransaction(String sql) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            log.error("No active transaction for sql {}, stack {}", sql, getStackTrace());
        }
    }

    static String getStackTrace() {
        try (StringWriter sw = new StringWriter()) {
            new Exception().printStackTrace(new PrintWriter(sw, true));
            return sw.getBuffer().toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isSelectSql(String sql) {
        return sql.toLowerCase().startsWith("select");
    }
}