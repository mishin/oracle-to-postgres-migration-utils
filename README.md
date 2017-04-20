# Oracle RDBMS to PostgreSQL migration utilities

## В репозитории собраны Java классы, полезные при миграции с Oracle на Postgres
* TransactionCheckJdbcTemplate и TransactionCheckNamedParameterJdbcTemplate - Spring templates с дополнительной 
проверкой того, что явно открыта транзакция работы с БД.
* BiTransactionTemplate - быстрое решение для синхронизации транзакций в двух БД.  
!Не содержит логики полноценной распределённой транзакции. 
Использовалась нами только в момент переключения между БД. 