"""Tests for the full_text_search (inverted index) and geo_within_radius /
geo_within_bbox (geo k-d tree) bindings — thin PyO3 wrappers over the existing
core methods. Result is a list of node ids, ascending."""

from rustychickpeas import GraphSnapshotBuilder, Direction


def _graph():
    # Three creative works (titled / described) mentioning two geonames Features.
    b = GraphSnapshotBuilder()
    b.add_node(["CreativeWork"], node_id=0)
    b.set_prop(0, "title", "London derby")
    b.set_prop(0, "description", "a football match in london")
    b.add_node(["CreativeWork"], node_id=1)
    b.set_prop(1, "title", "Wimbledon final")
    b.set_prop(1, "description", "a tennis championship")
    b.add_node(["CreativeWork"], node_id=2)
    b.set_prop(2, "title", "Football weekly")
    b.set_prop(2, "description", "sports roundup")
    # Features with wgs84 coords: London and Paris.
    b.add_node(["Feature"], node_id=3)
    b.set_prop(3, "lat", 51.5074)
    b.set_prop(3, "long", -0.1278)
    b.add_node(["Feature"], node_id=4)
    b.set_prop(4, "lat", 48.8566)
    b.set_prop(4, "long", 2.3522)
    b.add_relationship(0, 3, "mentions")  # London derby -> London
    b.add_relationship(1, 3, "mentions")  # Wimbledon -> London
    b.add_relationship(2, 4, "mentions")  # Football weekly -> Paris
    return b.finalize()


def test_full_text_search_title_and_description():
    g = _graph()
    # 'football' is in cw0's description and cw2's title.
    assert g.full_text_search("CreativeWork", "title", "football") == [2]
    assert g.full_text_search("CreativeWork", "description", "football") == [0]
    # union over the two fields (basic q8 / a20 shape)
    hits = set(g.full_text_search("CreativeWork", "title", "football")) | set(
        g.full_text_search("CreativeWork", "description", "football")
    )
    assert hits == {0, 2}
    # whole-word, case-insensitive
    assert g.full_text_search("CreativeWork", "title", "WIMBLEDON") == [1]
    # a token nobody has -> empty
    assert g.full_text_search("CreativeWork", "title", "cricket") == []


def test_geo_within_radius():
    g = _graph()
    # within 50km of London -> the London feature (id 3) only.
    near = g.geo_within_radius("Feature", "lat", "long", 51.5074, -0.1278, 50.0)
    assert near == [3]
    # widen to cover Paris (~340km) -> both features, ascending.
    both = g.geo_within_radius("Feature", "lat", "long", 51.5074, -0.1278, 500.0)
    assert both == [3, 4]


def test_geo_within_bbox():
    g = _graph()
    # +/-1 deg box around London covers London only.
    box = g.geo_within_bbox("Feature", "lat", "long", 50.5, -1.13, 52.5, 0.87)
    assert box == [3]
    # widen the box to include Paris.
    wide = g.geo_within_bbox("Feature", "lat", "long", 48.0, -1.0, 52.0, 3.0)
    assert wide == [3, 4]


def test_geo_then_reverse_traversal():
    g = _graph()
    # a17 shape: works mentioning an in-box Feature.
    works = set()
    for f in g.geo_within_bbox("Feature", "lat", "long", 50.5, -1.13, 52.5, 0.87):
        works.update(g.neighbor_ids(f, Direction.Incoming, ["mentions"]))
    assert works == {0, 1}  # the two London works, not the Paris one


def test_unknown_label_or_key_is_empty():
    g = _graph()
    assert g.full_text_search("NoSuchLabel", "title", "football") == []
    assert g.full_text_search("CreativeWork", "nokey", "football") == []
    assert g.geo_within_radius("NoSuchLabel", "lat", "long", 0.0, 0.0, 10.0) == []
