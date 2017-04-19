package ru.yandex.money.server.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;

/**
 * TransactionStatus объеденяющий в работу с двумя статусами oracle и postgres, нужен для работы с вложеными транзакциями - для миграции.
 * <p>
 * Created by kalashnikov on 10/5/16.
 */
public class BiTransactionStatus implements TransactionStatus {

    private static final Logger log = LoggerFactory.getLogger(BiTransactionStatus.class);

    private final TransactionStatus oracleTransactionStatus;
    private final TransactionStatus postgresTransactionStatus;

    private BiTransactionStatus(TransactionStatus oracleTransactionStatus, TransactionStatus postgresTransactionStatus) {
        this.oracleTransactionStatus = oracleTransactionStatus;
        this.postgresTransactionStatus = postgresTransactionStatus;
    }

    /**
     * В нашем коде не используется. Сделана символическая заглушка.
     *
     * @return признак новой транзакции если хотя бы одна из транзакций новая(хотя они должны быть одинаковы).
     */
    @Override
    public boolean isNewTransaction() {
        logIfNotSame("isNewTransaction", oracleTransactionStatus.isNewTransaction(), postgresTransactionStatus.isNewTransaction());
        return oracleTransactionStatus.isNewTransaction() || postgresTransactionStatus.isNewTransaction();
    }

    /**
     * В нашем коде не используется. Сделана символическая заглушка.
     *
     * @return признак наличия savepoint хотя бы в одной из транзакций(хотя они должны быть одинаковы).
     */
    @Override
    public boolean hasSavepoint() {
        logIfNotSame("hasSavepoint", oracleTransactionStatus.hasSavepoint(), postgresTransactionStatus.hasSavepoint());
        return oracleTransactionStatus.hasSavepoint() || postgresTransactionStatus.hasSavepoint();
    }

    /**
     * Метод ради которого сделан этот класс, прозрачно для внешнего клиента устанавливает флаг отката в обе транзакции.
     */
    @Override
    public void setRollbackOnly() {
        oracleTransactionStatus.setRollbackOnly();
        postgresTransactionStatus.setRollbackOnly();
    }

    /**
     * В нашем коде не используется. Сделана символическая заглушка.
     *
     * @return признак наличия флага отката транзакции хотя бы в одной из транзакций(хотя они должны быть одинаковы).
     */
    @Override
    public boolean isRollbackOnly() {
        logIfNotSame("isRollbackOnly", oracleTransactionStatus.isRollbackOnly(), postgresTransactionStatus.isRollbackOnly());
        return oracleTransactionStatus.isRollbackOnly() || postgresTransactionStatus.isRollbackOnly();
    }

    /**
     * В нашем коде не используется. Сделана символическая заглушка.
     * Проталкивает изменения в datasource в обе транзакции.
     */
    @Override
    public void flush() {
        oracleTransactionStatus.flush();
        postgresTransactionStatus.flush();
    }

    /**
     * В нашем коде не используется. Сделана символическая заглушка.
     *
     * @return признак завершенности транзакции хотя бы в одной из транзакций(хотя они должны быть одинаковы).
     */
    @Override
    public boolean isCompleted() {
        logIfNotSame("isCompleted", oracleTransactionStatus.isCompleted(), postgresTransactionStatus.isCompleted());
        return oracleTransactionStatus.isCompleted() && postgresTransactionStatus.isCompleted();
    }

    /**
     * В нашем коде не используется. Сделана символическая заглушка.
     * Создает составной savepoint для двух транзакций.
     *
     * @return savepoint для двух транзакций
     * @throws TransactionException исключения при работе с транзакцией
     */
    @Override
    public Object createSavepoint() throws TransactionException {
        return new BiSavepoint(oracleTransactionStatus.createSavepoint(), postgresTransactionStatus.createSavepoint());
    }

    /**
     * В нашем коде не используется. Сделана символическая заглушка.
     * Откат до savepoint. Если savepoint составной до откатываем обе транзакции.
     */
    @Override
    public void rollbackToSavepoint(Object savepoint) throws TransactionException {
        if (savepoint instanceof BiSavepoint) {
            BiSavepoint biSavepoint = (BiSavepoint) savepoint;
            oracleTransactionStatus.rollbackToSavepoint(biSavepoint.oracleSavePoint);
            postgresTransactionStatus.rollbackToSavepoint(biSavepoint.postgresSavePoint);
        } else {
            oracleTransactionStatus.rollbackToSavepoint(savepoint);
        }
    }

    /**
     * В нашем коде не используется. Сделана символическая заглушка.
     * Освобождение savepoint. Если savepoint составной до освобождаем обе транзакции.
     */
    @Override
    public void releaseSavepoint(Object savepoint) throws TransactionException {
        if (savepoint instanceof BiSavepoint) {
            BiSavepoint biSavepoint = (BiSavepoint) savepoint;
            oracleTransactionStatus.releaseSavepoint(biSavepoint.oracleSavePoint);
            postgresTransactionStatus.releaseSavepoint(biSavepoint.postgresSavePoint);
        } else {
            oracleTransactionStatus.releaseSavepoint(savepoint);
        }
    }

    /**
     * Метод создания BiTransactionStatus.
     *
     * @param oracleTransactionStatus   оракловый статус
     * @param postgresTransactionStatus постгрисовый статус
     * @return BiTransactionStatus
     */
    public static BiTransactionStatus of(TransactionStatus oracleTransactionStatus, TransactionStatus postgresTransactionStatus) {
        return new BiTransactionStatus(oracleTransactionStatus, postgresTransactionStatus);
    }

    private static void logIfNotSame(String action, boolean oracleFlag, boolean postgresFlag) {
        if (oracleFlag != postgresFlag) {
            log.warn("{} is not same : oracle={}, postgres={}", action, oracleFlag, postgresFlag);
        }
    }

    private static class BiSavepoint {
        private Object oracleSavePoint;
        private Object postgresSavePoint;

        private BiSavepoint(Object oracleSavePoint, Object postgresSavePoint) {
            this.oracleSavePoint = oracleSavePoint;
            this.postgresSavePoint = postgresSavePoint;
        }
    }
}
