# Data Quality Framework Implementation Timeline

**Project:** Scout Data Pipeline  
**Feature:** Comprehensive Data Quality Framework  
**Status:** Planning Phase  
**Created:** 2025-11-22

---

## Overview

This document outlines the implementation timeline for a comprehensive Data Quality Framework that includes:
1. Schema validation before ClickHouse inserts
2. Completeness checks
3. Freshness monitoring
4. Data quality metrics in daily listings

---

## Current State Analysis

### ✅ Existing Components
- **Basic Validation**: `DataQualityChecker` class exists (`src/utils/validation.py`)
  - Validates general stats, player stats, goal events, shot events
  - Used during bronze scraping (optional, configurable)
  - Basic checks: required fields, null values, data types, duplicates

### ⚠️ Gaps Identified
- No schema validation before ClickHouse insertion
- No completeness scoring/metrics
- No freshness monitoring (data age tracking)
- No data quality metrics stored in daily listings
- Limited validation for AIScore data
- No validation at ClickHouse load stage

---

## Implementation Timeline

### Phase 1: Foundation & Schema Validation (Week 1-2)

#### Week 1: Schema Validation Framework
**Duration:** 5 days  
**Effort:** 2-3 days development + 2 days testing

**Tasks:**
1. **Day 1-2: Create Schema Registry**
   - Create `src/utils/schema_registry.py`
   - Define schema definitions for all ClickHouse tables (FotMob + AIScore)
   - Support for:
     - Column names and types
     - Nullable constraints
     - Value ranges/constraints
     - Foreign key relationships
   - **Deliverable:** Schema registry module with all table schemas

2. **Day 3: Schema Validator Class**
   - Create `src/utils/schema_validator.py`
   - Implement `SchemaValidator` class:
     - `validate_dataframe(df, table_name, database)` method
     - Type checking (pandas → ClickHouse type mapping)
     - Nullable field validation
     - Constraint validation (ranges, formats)
     - Generate detailed validation reports
   - **Deliverable:** Schema validator with comprehensive checks

3. **Day 4: Integration with Load Script**
   - Integrate schema validation into `scripts/load_clickhouse.py`
   - Add validation step before `insert_dataframe()` calls
   - Handle validation failures:
     - Log detailed errors
     - Option to skip invalid rows vs fail entire batch
     - Generate validation reports
   - **Deliverable:** Schema validation integrated into data loading

4. **Day 5: Testing & Documentation**
   - Unit tests for schema validator
   - Integration tests with sample data
   - Test edge cases (type mismatches, null violations, etc.)
   - Update documentation
   - **Deliverable:** Tested and documented schema validation

**Success Criteria:**
- ✅ All tables have schema definitions
- ✅ Validation runs before every ClickHouse insert
- ✅ Validation errors are logged with details
- ✅ Tests cover all validation scenarios

---

### Phase 2: Completeness Checks (Week 2-3)

#### Week 2: Completeness Scoring System
**Duration:** 5 days  
**Effort:** 3 days development + 2 days testing

**Tasks:**
1. **Day 1-2: Completeness Calculator**
   - Create `src/utils/completeness.py`
   - Implement `CompletenessCalculator` class:
     - Field-level completeness (non-null percentage)
     - Record-level completeness (required fields present)
     - Table-level completeness score
     - Weighted completeness (important fields weighted higher)
   - Support for both FotMob and AIScore data
   - **Deliverable:** Completeness calculation module

2. **Day 3: Completeness Checks Integration**
   - Add completeness checks to `scripts/load_clickhouse.py`
   - Calculate completeness scores:
     - Per-match completeness
     - Per-table completeness
     - Per-date completeness
   - Store completeness metrics:
     - In memory during processing
     - In daily listings JSON
   - **Deliverable:** Completeness checks integrated

3. **Day 4: Completeness Reporting**
   - Generate completeness reports:
     - Summary statistics
     - Field-level breakdowns
     - Historical trends
   - Add completeness alerts:
     - Alert if completeness drops below threshold
     - Alert if specific fields are consistently incomplete
   - **Deliverable:** Completeness reporting and alerting

4. **Day 5: Testing & Refinement**
   - Test with various data completeness scenarios
   - Validate completeness calculations
   - Test alerting thresholds
   - **Deliverable:** Tested completeness system

**Success Criteria:**
- ✅ Completeness scores calculated for all data
- ✅ Scores stored in daily listings
- ✅ Alerts triggered for low completeness
- ✅ Reports generated and accessible

---

### Phase 3: Freshness Monitoring (Week 3-4)

#### Week 3: Freshness Tracking System
**Duration:** 5 days  
**Effort:** 3 days development + 2 days testing

**Tasks:**
1. **Day 1-2: Freshness Monitor**
   - Create `src/utils/freshness.py`
   - Implement `FreshnessMonitor` class:
     - Track data age (time since scrape)
     - Track data staleness (time since last update)
     - Calculate freshness scores
     - Define freshness thresholds:
       - Fresh: < 1 hour
       - Stale: 1-24 hours
       - Very stale: > 24 hours
   - **Deliverable:** Freshness monitoring module

2. **Day 3: Freshness Integration**
   - Add freshness tracking to:
     - Bronze layer (scrape timestamps)
     - ClickHouse loading (load timestamps)
     - Daily listings (last update times)
   - Track freshness metrics:
     - Per-match freshness
     - Per-date freshness
     - Overall pipeline freshness
   - **Deliverable:** Freshness tracking integrated

3. **Day 4: Freshness Alerts & Reporting**
   - Implement freshness alerts:
     - Alert if data is stale (> threshold)
     - Alert if pipeline hasn't run in X hours
     - Alert if specific dates are missing updates
   - Generate freshness reports:
     - Current freshness status
     - Historical freshness trends
     - Staleness patterns
   - **Deliverable:** Freshness alerting and reporting

4. **Day 5: Testing & Documentation**
   - Test freshness calculations
   - Test alert thresholds
   - Test with various time scenarios
   - Document freshness metrics
   - **Deliverable:** Tested freshness system

**Success Criteria:**
- ✅ Freshness tracked for all data
- ✅ Alerts triggered for stale data
- ✅ Freshness metrics in daily listings
- ✅ Reports available

---

### Phase 4: Data Quality Metrics in Daily Listings (Week 4)

#### Week 4: Daily Listings Integration
**Duration:** 5 days  
**Effort:** 2 days development + 2 days testing + 1 day documentation

**Tasks:**
1. **Day 1: Data Quality Metrics Structure**
   - Design data quality metrics schema for daily listings
   - Define metrics to include:
     - Schema validation results (pass/fail, error count)
     - Completeness scores (per table, overall)
     - Freshness metrics (average age, staleness count)
     - Data volume metrics (rows inserted, size)
     - Quality score (composite score 0-100)
   - **Deliverable:** Metrics schema design

2. **Day 2: Metrics Collection & Storage**
   - Create `src/utils/data_quality_metrics.py`
   - Implement `DataQualityMetrics` class:
     - Collect metrics during processing
     - Aggregate metrics per date
     - Store metrics in daily listings JSON
   - Update `src/storage/bronze_storage.py`:
     - Add data quality section to daily listings
     - Store metrics after processing
   - **Deliverable:** Metrics collection and storage

3. **Day 3: Metrics Aggregation**
   - Aggregate metrics:
     - Per-scraper (FotMob vs AIScore)
     - Per-table
     - Per-date
     - Overall pipeline
   - Calculate composite quality score:
     - Weighted combination of schema, completeness, freshness
     - Score range: 0-100
   - **Deliverable:** Metrics aggregation system

4. **Day 4: Testing & Validation**
   - Test metrics collection
   - Test metrics storage in daily listings
   - Test metrics aggregation
   - Validate quality score calculations
   - **Deliverable:** Tested metrics system

5. **Day 5: Documentation & Examples**
   - Document data quality metrics structure
   - Create examples of metrics in daily listings
   - Update README with data quality section
   - **Deliverable:** Complete documentation

**Success Criteria:**
- ✅ Data quality metrics stored in daily listings
- ✅ Metrics accessible via JSON files
- ✅ Composite quality score calculated
- ✅ Documentation complete

---

## Detailed Task Breakdown

### Phase 1: Schema Validation

#### Task 1.1: Schema Registry
**Files to Create:**
- `src/utils/schema_registry.py`

**Schema Definition Format:**
```python
FOTMOB_SCHEMAS = {
    'general': {
        'columns': {
            'match_id': {'type': 'Int32', 'nullable': False},
            'league_name': {'type': 'String', 'nullable': True},
            'home_team_id': {'type': 'Int32', 'nullable': False},
            # ... all columns
        },
        'required_fields': ['match_id', 'home_team_id', 'away_team_id'],
        'constraints': {
            'match_id': {'min': 1},
            'fotmob_rating': {'min': 0, 'max': 10}
        }
    },
    # ... all tables
}
```

**Estimated Effort:** 1 day

#### Task 1.2: Schema Validator
**Files to Create:**
- `src/utils/schema_validator.py`

**Key Methods:**
- `validate_dataframe(df, table_name, database) -> ValidationResult`
- `validate_types(df, schema) -> List[Error]`
- `validate_constraints(df, schema) -> List[Error]`
- `validate_nullable(df, schema) -> List[Error]`

**Estimated Effort:** 1 day

#### Task 1.3: Integration
**Files to Modify:**
- `scripts/load_clickhouse.py`

**Integration Points:**
- Before `insert_dataframe()` calls
- In `load_fotmob_data()` function
- In `load_aiscore_data()` function

**Estimated Effort:** 1 day

---

### Phase 2: Completeness Checks

#### Task 2.1: Completeness Calculator
**Files to Create:**
- `src/utils/completeness.py`

**Key Methods:**
- `calculate_field_completeness(df, field) -> float`
- `calculate_record_completeness(df, required_fields) -> pd.Series`
- `calculate_table_completeness(df, schema) -> float`
- `calculate_weighted_completeness(df, weights) -> float`

**Estimated Effort:** 2 days

#### Task 2.2: Integration
**Files to Modify:**
- `scripts/load_clickhouse.py`
- `src/storage/bronze_storage.py`

**Integration Points:**
- After data processing, before insertion
- Store completeness scores in daily listings

**Estimated Effort:** 1 day

---

### Phase 3: Freshness Monitoring

#### Task 3.1: Freshness Monitor
**Files to Create:**
- `src/utils/freshness.py`

**Key Methods:**
- `calculate_data_age(scrape_timestamp) -> timedelta`
- `calculate_freshness_score(age, thresholds) -> float`
- `check_staleness(timestamp, threshold) -> bool`
- `get_freshness_status(age) -> str`

**Estimated Effort:** 2 days

#### Task 3.2: Integration
**Files to Modify:**
- `scripts/load_clickhouse.py`
- `src/storage/bronze_storage.py`
- `src/orchestrator.py`

**Integration Points:**
- Track scrape timestamps
- Track load timestamps
- Calculate freshness during processing

**Estimated Effort:** 1 day

---

### Phase 4: Daily Listings Integration

#### Task 4.1: Metrics Structure
**Daily Listings JSON Structure:**
```json
{
  "date": "20251115",
  "scraped_at": "2025-11-15T10:00:00",
  "data_quality": {
    "overall_score": 95.5,
    "schema_validation": {
      "passed": true,
      "errors": 0,
      "warnings": 2,
      "tables_validated": 14
    },
    "completeness": {
      "overall": 98.2,
      "by_table": {
        "general": 100.0,
        "player": 97.5,
        "goal": 96.8
      },
      "missing_fields": []
    },
    "freshness": {
      "average_age_hours": 0.5,
      "stale_records": 0,
      "last_update": "2025-11-15T10:30:00"
    },
    "volume": {
      "rows_inserted": 1250,
      "tables_loaded": 14,
      "total_size_mb": 45.2
    }
  }
}
```

**Estimated Effort:** 0.5 day

#### Task 4.2: Metrics Collection
**Files to Create:**
- `src/utils/data_quality_metrics.py`

**Files to Modify:**
- `scripts/load_clickhouse.py`
- `src/storage/bronze_storage.py`

**Estimated Effort:** 1.5 days

---

## Resource Requirements

### Development Team
- **1 Senior Data Engineer** (full-time, 4 weeks)
- **1 Junior Data Engineer** (part-time, 2 weeks for testing)

### Infrastructure
- No additional infrastructure needed
- Uses existing ClickHouse and storage systems

### Dependencies
- Existing: pandas, clickhouse-connect, pydantic
- No new dependencies required

---

## Risk Assessment

### High Risk
- **Schema Evolution**: API changes may break schema validation
  - **Mitigation**: Version schemas, support multiple schema versions
  - **Timeline Impact:** +2 days for schema versioning

### Medium Risk
- **Performance Impact**: Validation may slow down data loading
  - **Mitigation**: Optimize validation, make it configurable
  - **Timeline Impact:** +1 day for optimization

- **False Positives**: Completeness checks may flag valid sparse data
  - **Mitigation**: Configurable thresholds, field-level weights
  - **Timeline Impact:** +1 day for tuning

### Low Risk
- **Testing Complexity**: Many edge cases to test
  - **Mitigation**: Comprehensive test suite
  - **Timeline Impact:** Already included in timeline

---

## Success Metrics

### Phase 1 Success Criteria
- ✅ 100% of ClickHouse inserts validated before insertion
- ✅ < 1% false positive rate for schema validation
- ✅ Validation errors logged with actionable details

### Phase 2 Success Criteria
- ✅ Completeness scores calculated for all data
- ✅ Completeness thresholds configurable
- ✅ Completeness alerts working correctly

### Phase 3 Success Criteria
- ✅ Freshness tracked for all data
- ✅ Freshness alerts triggered appropriately
- ✅ Freshness reports generated

### Phase 4 Success Criteria
- ✅ Data quality metrics in all daily listings
- ✅ Composite quality score calculated
- ✅ Metrics accessible via JSON files

---

## Timeline Summary

| Phase | Duration | Start Date | End Date | Key Deliverables |
|-------|----------|-----------|----------|------------------|
| Phase 1: Schema Validation | 2 weeks | Week 1 | Week 2 | Schema registry, validator, integration |
| Phase 2: Completeness | 1 week | Week 2 | Week 3 | Completeness calculator, integration, reporting |
| Phase 3: Freshness | 1 week | Week 3 | Week 4 | Freshness monitor, integration, alerts |
| Phase 4: Daily Listings | 1 week | Week 4 | Week 5 | Metrics collection, storage, documentation |
| **Total** | **4-5 weeks** | **Week 1** | **Week 5** | **Complete Data Quality Framework** |

---

## Post-Implementation

### Week 6: Monitoring & Refinement
- Monitor data quality metrics in production
- Tune thresholds based on real data
- Refine completeness weights
- Optimize validation performance
- Gather user feedback

### Future Enhancements
- Data quality dashboard (Grafana/Tableau)
- Automated data quality reports (email/Slack)
- Data quality trends analysis
- Anomaly detection for quality metrics
- Integration with Great Expectations or similar tools

---

## Notes

- All phases can be implemented incrementally
- Each phase can be tested independently
- Schema validation is the highest priority (prevents bad data)
- Completeness and freshness add value but are less critical
- Daily listings integration ties everything together

---

**Document Version:** 1.0  
**Last Updated:** 2025-11-22  
**Next Review:** After Phase 1 completion

