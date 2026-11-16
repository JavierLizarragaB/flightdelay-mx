import pytest

from src.gold.feature_config import (
    FORBIDDEN_SOURCE_COLUMNS,
    MODEL_FEATURES,
    TARGET,
    LeakageError,
    assert_no_leakage,
    is_forbidden,
)


def test_model_features_have_no_leakage():
    assert_no_leakage(MODEL_FEATURES)


def test_target_is_not_a_feature():
    assert TARGET not in MODEL_FEATURES


def test_model_features_are_unique():
    assert len(MODEL_FEATURES) == len(set(MODEL_FEATURES))


@pytest.mark.parametrize("col", sorted(FORBIDDEN_SOURCE_COLUMNS))
def test_every_forbidden_column_is_rejected(col):
    with pytest.raises(LeakageError):
        assert_no_leakage([*MODEL_FEATURES, col])


@pytest.mark.parametrize(
    "col", ["dep_delay", "DEP_DELAY", "taxi_out", "arr_del15", "Div1Airport", "div3_tail_num"]
)
def test_detection_ignores_naming_convention(col):
    with pytest.raises(LeakageError):
        assert_no_leakage([col])


@pytest.mark.parametrize(
    "col", ["distance", "crs_elapsed_time", "route_delay_rate_30d", "dep_hour_sin", "airline"]
)
def test_legit_columns_pass(col):
    # `distance` empieza con "di" pero no debe confundirse con las columnas Div*.
    assert not is_forbidden(col)
