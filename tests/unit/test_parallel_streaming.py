"""
Unit tests for parallel streaming components.

Tests the BlockRangePartitionStrategy and ParallelConfig classes for correct
partition creation and CTE-based query wrapping.
"""

import pytest

from src.amp.streaming.parallel import (
    BlockRangePartitionStrategy,
    ParallelConfig,
    QueryPartition,
)


@pytest.mark.unit
class TestQueryPartition:
    """Test QueryPartition dataclass"""

    def test_partition_creation(self):
        """Test creating a query partition"""
        partition = QueryPartition(partition_id=0, start_block=0, end_block=1000000, block_column='block_num')

        assert partition.partition_id == 0
        assert partition.start_block == 0
        assert partition.end_block == 1000000
        assert partition.block_column == 'block_num'

    def test_partition_metadata(self):
        """Test partition metadata generation"""
        partition = QueryPartition(partition_id=2, start_block=500000, end_block=750000, block_column='_block_num')

        metadata = partition.metadata
        assert metadata['start_block'] == 500000
        assert metadata['end_block'] == 750000
        assert metadata['block_column'] == '_block_num'
        assert metadata['partition_size'] == 250000

    def test_default_block_column(self):
        """Test default block column name"""
        partition = QueryPartition(partition_id=0, start_block=0, end_block=1000)

        assert partition.block_column == 'block_num'


@pytest.mark.unit
class TestParallelConfig:
    """Test ParallelConfig validation"""

    def test_valid_config(self):
        """Test creating valid parallel configuration"""
        config = ParallelConfig(
            num_workers=8, min_block=0, max_block=1_000_000, table_name='blocks', block_column='block_num'
        )

        assert config.num_workers == 8
        assert config.min_block == 0
        assert config.max_block == 1_000_000
        assert config.table_name == 'blocks'
        assert config.block_column == 'block_num'
        assert config.partition_size is None
        assert config.stop_on_error is False

    def test_invalid_num_workers(self):
        """Test that num_workers < 1 raises ValueError"""
        with pytest.raises(ValueError, match='num_workers must be >= 1'):
            ParallelConfig(num_workers=0, min_block=0, max_block=1000, table_name='blocks')

    def test_invalid_block_range(self):
        """Test that min_block >= max_block raises ValueError"""
        with pytest.raises(ValueError, match='min_block .* must be < max_block'):
            ParallelConfig(num_workers=4, min_block=1000, max_block=1000, table_name='blocks')

        with pytest.raises(ValueError, match='min_block .* must be < max_block'):
            ParallelConfig(num_workers=4, min_block=2000, max_block=1000, table_name='blocks')

    def test_invalid_partition_size(self):
        """Test that partition_size < 1 raises ValueError"""
        with pytest.raises(ValueError, match='partition_size must be >= 1'):
            ParallelConfig(num_workers=4, min_block=0, max_block=1000, table_name='blocks', partition_size=0)

    def test_missing_table_name(self):
        """Test that empty table_name raises ValueError"""
        with pytest.raises(ValueError, match='table_name is required'):
            ParallelConfig(num_workers=4, min_block=0, max_block=1000, table_name='')

    def test_custom_block_column(self):
        """Test custom block column name"""
        config = ParallelConfig(num_workers=4, min_block=0, max_block=1000, table_name='txs', block_column='_block_num')

        assert config.block_column == '_block_num'

    def test_optional_max_block(self):
        """Test that max_block can be None for hybrid streaming"""
        config = ParallelConfig(num_workers=4, min_block=0, max_block=None, table_name='blocks')

        assert config.num_workers == 4
        assert config.min_block == 0
        assert config.max_block is None
        assert config.table_name == 'blocks'

    def test_default_min_block(self):
        """Test that min_block defaults to 0"""
        config = ParallelConfig(num_workers=4, max_block=1000, table_name='blocks')

        assert config.min_block == 0
        assert config.max_block == 1000

    def test_both_blocks_optional(self):
        """Test config with only num_workers and table_name (hybrid mode)"""
        config = ParallelConfig(num_workers=8, table_name='blocks')

        assert config.min_block == 0
        assert config.max_block is None
        assert config.num_workers == 8

    def test_validation_skipped_when_max_block_none(self):
        """Test that min_block >= max_block validation is skipped when max_block is None"""
        # This should not raise even though min_block would be >= max_block if max_block were 0
        config = ParallelConfig(num_workers=4, min_block=1000, max_block=None, table_name='blocks')

        assert config.min_block == 1000
        assert config.max_block is None


@pytest.mark.unit
class TestBlockRangePartitionStrategy:
    """Test BlockRangePartitionStrategy for partition creation and query wrapping"""

    def test_create_partitions_auto_sized(self):
        """Test partition creation with auto-calculated partition size"""
        strategy = BlockRangePartitionStrategy(table_name='blocks', block_column='block_num')

        config = ParallelConfig(num_workers=4, min_block=0, max_block=1_000_000, table_name='blocks')

        partitions = strategy.create_partitions(config)

        # Should create 4 partitions of 250k blocks each
        assert len(partitions) == 4
        assert partitions[0].start_block == 0
        assert partitions[0].end_block == 250_000
        assert partitions[1].start_block == 250_000
        assert partitions[1].end_block == 500_000
        assert partitions[2].start_block == 500_000
        assert partitions[2].end_block == 750_000
        assert partitions[3].start_block == 750_000
        assert partitions[3].end_block == 1_000_000

    def test_create_partitions_fixed_size(self):
        """Test partition creation with fixed partition size"""
        strategy = BlockRangePartitionStrategy(table_name='blocks', block_column='block_num')

        config = ParallelConfig(
            num_workers=8, min_block=0, max_block=1_000_000, table_name='blocks', partition_size=100_000
        )

        partitions = strategy.create_partitions(config)

        # Should create 10 partitions of 100k blocks each
        assert len(partitions) == 10
        assert all(p.end_block - p.start_block == 100_000 for p in partitions)

    def test_create_partitions_uneven_division(self):
        """Test partition creation when block range doesn't divide evenly"""
        strategy = BlockRangePartitionStrategy(table_name='blocks', block_column='block_num')

        config = ParallelConfig(num_workers=3, min_block=0, max_block=1000, table_name='blocks')

        partitions = strategy.create_partitions(config)

        # Should create 3 partitions: [0-334), [334-668), [668-1000)
        assert len(partitions) == 3
        assert partitions[0].start_block == 0
        assert partitions[0].end_block == 334
        assert partitions[1].start_block == 334
        assert partitions[1].end_block == 668
        assert partitions[2].start_block == 668
        assert partitions[2].end_block == 1000

    def test_create_partitions_with_offset(self):
        """Test partition creation with non-zero starting block"""
        strategy = BlockRangePartitionStrategy(table_name='blocks', block_column='block_num')

        config = ParallelConfig(num_workers=4, min_block=1_000_000, max_block=2_000_000, table_name='blocks')

        partitions = strategy.create_partitions(config)

        assert len(partitions) == 4
        assert partitions[0].start_block == 1_000_000
        assert partitions[0].end_block == 1_250_000
        assert partitions[3].start_block == 1_750_000
        assert partitions[3].end_block == 2_000_000

    def test_create_partitions_small_range(self):
        """Test partition creation with small block range"""
        strategy = BlockRangePartitionStrategy(table_name='blocks', block_column='block_num')

        config = ParallelConfig(num_workers=4, min_block=0, max_block=10, table_name='blocks')

        partitions = strategy.create_partitions(config)

        # Should still create 4 partitions, each with at least 1 block
        assert len(partitions) == 4
        assert partitions[0].start_block == 0
        assert partitions[0].end_block == 3
        assert partitions[3].start_block == 9
        assert partitions[3].end_block == 10

    def test_create_partitions_custom_block_column(self):
        """Test partition creation with custom block column"""
        strategy = BlockRangePartitionStrategy(table_name='transactions', block_column='_block_num')

        config = ParallelConfig(
            num_workers=2, min_block=0, max_block=1000, table_name='transactions', block_column='_block_num'
        )

        partitions = strategy.create_partitions(config)

        assert len(partitions) == 2
        assert all(p.block_column == '_block_num' for p in partitions)

    def test_wrap_query_simple_select(self):
        """Test WHERE clause injection for simple SELECT query"""
        strategy = BlockRangePartitionStrategy(table_name='blocks', block_column='block_num')

        partition = QueryPartition(partition_id=0, start_block=0, end_block=1_000_000, block_column='block_num')

        user_query = 'SELECT * FROM blocks'
        wrapped_query = strategy.wrap_query_with_partition(user_query, partition)

        expected = 'SELECT * FROM blocks WHERE block_num >= 0 AND block_num < 1000000'

        assert wrapped_query == expected

    def test_wrap_query_with_where_clause(self):
        """Test WHERE clause injection for query with existing WHERE clause"""
        strategy = BlockRangePartitionStrategy(table_name='blocks', block_column='block_num')

        partition = QueryPartition(partition_id=1, start_block=1_000_000, end_block=2_000_000, block_column='block_num')

        user_query = 'SELECT * FROM blocks WHERE hash IS NOT NULL'
        wrapped_query = strategy.wrap_query_with_partition(user_query, partition)

        expected = 'SELECT * FROM blocks WHERE hash IS NOT NULL AND (block_num >= 1000000 AND block_num < 2000000)'

        assert wrapped_query == expected

    def test_wrap_query_strips_trailing_semicolon(self):
        """Test that trailing semicolon is removed before wrapping"""
        strategy = BlockRangePartitionStrategy(table_name='blocks', block_column='block_num')

        partition = QueryPartition(partition_id=0, start_block=0, end_block=1000, block_column='block_num')

        user_query = 'SELECT * FROM blocks;'
        wrapped_query = strategy.wrap_query_with_partition(user_query, partition)

        expected = 'SELECT * FROM blocks WHERE block_num >= 0 AND block_num < 1000'

        assert wrapped_query == expected

    def test_wrap_query_custom_block_column(self):
        """Test WHERE clause injection with custom block column name"""
        strategy = BlockRangePartitionStrategy(table_name='transactions', block_column='_block_num')

        partition = QueryPartition(partition_id=0, start_block=0, end_block=1000, block_column='_block_num')

        user_query = 'SELECT * FROM transactions'
        wrapped_query = strategy.wrap_query_with_partition(user_query, partition)

        expected = 'SELECT * FROM transactions WHERE _block_num >= 0 AND _block_num < 1000'

        assert wrapped_query == expected

    def test_wrap_query_large_block_numbers(self):
        """Test CTE wrapping with large block numbers"""
        strategy = BlockRangePartitionStrategy(table_name='blocks', block_column='block_num')

        partition = QueryPartition(
            partition_id=0, start_block=18_000_000, end_block=19_000_000, block_column='block_num'
        )

        user_query = 'SELECT * FROM blocks'
        wrapped_query = strategy.wrap_query_with_partition(user_query, partition)

        assert 'block_num >= 18000000' in wrapped_query
        assert 'block_num < 19000000' in wrapped_query

    def test_wrap_query_with_settings_clause(self):
        """Test WHERE clause injection for streaming query with SETTINGS clause"""
        strategy = BlockRangePartitionStrategy(table_name='blocks', block_column='block_num')

        partition = QueryPartition(partition_id=0, start_block=0, end_block=1000, block_column='block_num')

        user_query = 'SELECT * FROM blocks SETTINGS stream = true'
        wrapped_query = strategy.wrap_query_with_partition(user_query, partition)

        expected = 'SELECT * FROM blocks WHERE block_num >= 0 AND block_num < 1000 SETTINGS stream = true'

        assert wrapped_query == expected

    def test_partition_ids_are_sequential(self):
        """Test that partition IDs are assigned sequentially"""
        strategy = BlockRangePartitionStrategy(table_name='blocks', block_column='block_num')

        config = ParallelConfig(num_workers=5, min_block=0, max_block=1000, table_name='blocks')

        partitions = strategy.create_partitions(config)

        for i, partition in enumerate(partitions):
            assert partition.partition_id == i

    def test_partitions_cover_full_range(self):
        """Test that partitions completely cover the block range without gaps"""
        strategy = BlockRangePartitionStrategy(table_name='blocks', block_column='block_num')

        config = ParallelConfig(num_workers=7, min_block=100, max_block=1000, table_name='blocks')

        partitions = strategy.create_partitions(config)

        # Check first partition starts at min_block
        assert partitions[0].start_block == 100

        # Check last partition ends at max_block
        assert partitions[-1].end_block == 1000

        # Check no gaps between partitions
        for i in range(len(partitions) - 1):
            assert partitions[i].end_block == partitions[i + 1].start_block

    def test_partitions_dont_overlap(self):
        """Test that partitions don't overlap"""
        strategy = BlockRangePartitionStrategy(table_name='blocks', block_column='block_num')

        config = ParallelConfig(num_workers=4, min_block=0, max_block=1000, table_name='blocks')

        partitions = strategy.create_partitions(config)

        # Check no overlaps
        for i in range(len(partitions) - 1):
            assert partitions[i].end_block <= partitions[i + 1].start_block
