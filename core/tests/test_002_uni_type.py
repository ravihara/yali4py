from core.tests.samples.uni_type import ChildClass, GrandChildClass, ParentClass

parent01: ParentClass = ParentClass("parent-01")
parent02: ParentClass = ParentClass("parent-02")
child01: ChildClass = ChildClass("child-01")
child02: ChildClass = ChildClass("child-02")
grand_child01: GrandChildClass = GrandChildClass("grand-child-01")
grand_child02: GrandChildClass = GrandChildClass("grand-child-02")

p01_id = id(parent01)
p02_id = id(parent02)
c01_id = id(child01)
c02_id = id(child02)
g01_id = id(grand_child01)
g02_id = id(grand_child02)


def test_id_equality():
    assert p01_id == p02_id
    assert c01_id == c02_id
    assert g01_id == g02_id

    assert p01_id != c01_id
    assert p01_id != g01_id
    assert c01_id != g01_id


def test_singleton_id():
    assert p01_id == id(ParentClass.get_instance())
    assert c01_id == id(ChildClass.get_instance())
    assert g01_id == id(GrandChildClass.get_instance())

    assert isinstance(ParentClass.get_instance(), ParentClass)
    assert isinstance(ChildClass.get_instance(), ChildClass)
    assert isinstance(GrandChildClass.get_instance(), GrandChildClass)


def test_instance_values():
    p01_val = parent01.get_value()
    p02_val = parent02.get_value()
    c01_val = child01.get_value()
    c02_val = child02.get_value()
    g01_val = grand_child01.get_value()
    g02_val = grand_child02.get_value()

    assert p01_val == p02_val
    assert c01_val == c02_val
    assert g01_val == g02_val

    assert p01_val == "parent-01"
    assert c01_val == "child-01"
    assert g01_val == "grand-child-01"


def test_class_values():
    p01_val = parent01.get_base_value()
    p02_val = parent02.get_base_value()
    c01_val = child01.get_base_value()
    c02_val = child02.get_base_value()
    g01_val = grand_child01.get_base_value()
    g02_val = grand_child02.get_base_value()

    assert p01_val == p02_val
    assert c01_val == c02_val
    assert g01_val == g02_val

    assert p01_val == "Parent Value"
    assert c01_val == "Parent Value"
    assert g01_val == "Parent Value"
