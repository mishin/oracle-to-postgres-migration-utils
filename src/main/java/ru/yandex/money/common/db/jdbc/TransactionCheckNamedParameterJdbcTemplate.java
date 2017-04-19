package ru.yandex.money.common.db.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

/**
 * NamedParameterJdbcTemplate с проверкой наличия явно открытой транзакции при выполнении операций update, insert, delete
 * Created by vsozykin on 11.18.16.
 */
public class TransactionCheckNamedParameterJdbcTemplate extends NamedParameterJdbcTemplate {

    private static final Logger log = LoggerFactory.getLogger(TransactionCheckNamedParameterJdbcTemplate.class);

    public TransactionCheckNamedParameterJdbcTemplate(DataSource dataSource) {
        super(dataSource);
    }

    public TransactionCheckNamedParameterJdbcTemplate(JdbcOperations classicJdbcTemplate) {
        super(classicJdbcTemplate);
    }

    @Override
    public int update(String sql, SqlParameterSource paramSource) {
        requireOpenTransaction(sql);
        return super.update(sql, paramSource);
    }

    @Override
    public int update(String sql, Map<String, ?> paramMap) {
        requireOpenTransaction(sql);
        return super.update(sql, paramMap);
    }

    @Override
    public int update(String sql, SqlParameterSource paramSource, KeyHolder generatedKeyHolder) {
        requireOpenTransaction(sql);
        return super.update(sql, paramSource, generatedKeyHolder);
    }

    @Override
    public int update(String sql, SqlParameterSource paramSource, KeyHolder generatedKeyHolder, String[] keyColumnNames) {
        requireOpenTransaction(sql);
        return super.update(sql, paramSource, generatedKeyHolder, keyColumnNames);
    }

    @Override
    public int[] batchUpdate(String sql, Map<String, ?>[] batchValues) {
        requireOpenTransaction(sql);
        return super.batchUpdate(sql, batchValues);
    }

    @Override
    public int[] batchUpdate(String sql, SqlParameterSource[] batchArgs) {
        requireOpenTransaction(sql);
        return super.batchUpdate(sql, batchArgs);
    }

    @Override
    public <T> T execute(String sql, SqlParameterSource paramSource, PreparedStatementCallback<T> action) {
        if (!isSelectSql(sql)) {
            requireOpenTransaction(sql);
        }
        return super.execute(sql, paramSource, action);
    }

    @Override
    public <T> T execute(String sql, Map<String, ?> paramMap, PreparedStatementCallback<T> action) {
        if (!isSelectSql(sql)) {
            requireOpenTransaction(sql);
        }
        return super.execute(sql, paramMap, action);
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