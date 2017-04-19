package ru.yandex.money.server.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

/**
 * TransactionTemplate нужный для миграции. Содержит в себе два transactionTemplate для oracle и postgres
 * - для выполнения одной внутри другой.
 * <p>
 * Created by kalashnikov on 10/5/16.
 */
public class BiTransactionTemplate extends TransactionTemplate {

    private static final Logger log = LoggerFactory.getLogger(BiTransactionTemplate.class);

    private final TransactionTemplate oracleTransactionTemplate;

    private final TransactionTemplate postgresTransactionTemplate;

    public BiTransactionTemplate(TransactionTemplate oracleTransactionTemplate, TransactionTemplate postgresTransactionTemplate) {
        this.oracleTransactionTemplate = oracleTransactionTemplate;
        this.postgresTransactionTemplate = postgresTransactionTemplate;
    }

    /**
     * Выполняет логику callback внутри двух транзакций oracle и postgres. Postgres вложено в oracle
     *
     * @param callback логика выполняемая в транзакции
     * @param <T>      тип возвращаемого значения
     * @return результат выполнения callback
     */
    public <T> T execute(BiFunction<TransactionStatus, TransactionStatus, T> callback) throws TransactionException {
        AtomicBoolean postgresSuccessExecuted = new AtomicBoolean(false);
        try {
            return oracleTransactionTemplate.execute(oracleStatus -> {
                        T executeResult = postgresTransactionTemplate.execute(
                                postgresStatus ->
                                        callback.apply(oracleStatus, postgresStatus)
                        );
                        postgresSuccessExecuted.set(true);
                        return executeResult;
                    }
            );
        } catch (TransactionException ex) {
            if (postgresSuccessExecuted.get()) {
                log.warn("exception during oracle commit : {}", ex.getMessage());
            }
            throw ex;
        }
    }

    /**
     * Переопределенный метод выполнения транзакции. Выполняет action в двух транзакция oracle и postgres,
     * при этом передавая составной transactionStatus
     *
     * @param action выполняемое действие
     * @param <T>    тип возвращаемого значения
     * @return результат выполнения action
     * @throws TransactionException исключение при работе с транзакциями
     */
    @Override
    public <T> T execute(TransactionCallback<T> action) throws TransactionException {
        return execute((os, ps) -> action.doInTransaction(BiTransactionStatus.of(os, ps)));
    }

    /**
     * Настройка объекта после создания бина. Проверяет обязательность наличия обоих transactionTemplate.
     */
    @Override
    public void afterPropertiesSet() {
        if (oracleTransactionTemplate == null || postgresTransactionTemplate == null) {
            throw new IllegalArgumentException("Transaction templates is required");
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (!super.equals(obj)) {
            return false;
        }
        BiTransactionTemplate that = (BiTransactionTemplate) obj;
        return Objects.equals(oracleTransactionTemplate, that.oracleTransactionTemplate) &&
                Objects.equals(postgresTransactionTemplate, that.postgresTransactionTemplate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), oracleTransactionTemplate, postgresTransactionTemplate);
    }
}
