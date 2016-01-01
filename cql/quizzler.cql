CREATE KEYSPACE IF NOT EXISTS quizzler WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'SearchAnalytics': '1'
};

CREATE TABLE IF NOT EXISTS quizzler.question (
	question_id timeuuid,
    question text,
    category text,
    quizzler_percent int,
    PRIMARY KEY ((quizzler_percent, category), question_id)
) WITH CLUSTERING ORDER BY (question_id DESC);

CREATE TABLE IF NOT EXISTS quizzler.answers (
	answer_id timeuuid,
    question_id timeuuid,
    answer text,
    PRIMARY KEY (answer_id)
);
