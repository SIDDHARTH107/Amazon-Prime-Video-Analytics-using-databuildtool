-- This test ensures that all order amounts are positive
-- SELECT movie_id,
--        tag_id,
--        relevance_score
-- FROM {{ ref('fct_genome_scores') }}
-- WHERE relevance_score <= 0

-- Before the data is moved forward, we need to make sure that all our test cases pass.
-- If this test fails, it indicates that there are movies with a non-positive relevance score.

{{ no_nulls_in_columns(ref('fct_genome_scores')) }}