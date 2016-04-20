CREATE KEYSPACE IF NOT EXISTS quizzler WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE IF NOT EXISTS quizzler.account (
	username text,
    password text,
    quiz_interval decimal,
    last_quiz timestamp,
    chat_id text,
    chat_username text,
    PRIMARY KEY (username)
);

CREATE TABLE IF NOT EXISTS quizzler.question (
	question_id timeuuid,
    question text,
    category_name text,
    quizzler_percent int,
    PRIMARY KEY ((quizzler_percent, category_name), question_id)
) WITH CLUSTERING ORDER BY (question_id DESC);

CREATE TABLE IF NOT EXISTS quizzler.categories (
	category_id timeuuid,
    category_name text,
    PRIMARY KEY (category_id,category_name)
) WITH CLUSTERING ORDER BY (category_name DESC);

CREATE TABLE IF NOT EXISTS quizzler.answers (
	answer_id timeuuid,
    question_id timeuuid,
    answer text,
    PRIMARY KEY (answer_id)
);
