import pytest

from yali.core.strings import StringConv


@pytest.mark.parametrize(
    "input, output",
    [
        # With punctuation.
        ("Yali, toolkit!", "yaliToolkit"),
        # Camel cased
        ("yaliToolkit", "yaliToolkit"),
        # Joined by delimeter.
        ("Yali-Toolkit", "yaliToolkit"),
        # Cobol cased
        ("YALI-TOOLKIT", "yaliToolkit"),
        # Without punctuation.
        ("Yali toolkit", "yaliToolkit"),
        # Repeating single delimeter
        ("Yali   Toolkit", "yaliToolkit"),
        # Repeating delimeters of different types
        ("Yali -__  Toolkit", "yaliToolkit"),
        # Wrapped in delimeter
        (" yali toolkit ", "yaliToolkit"),
        # End in capital letter
        ("yalI", "yalI"),
        # Long sentence with punctuation
        (
            r"all work !nO play makes JACK @a duLl Gu'y",
            "allWorkNoPlayMakesJackADuLlGuy",
        ),
        # Alternating character cases
        ("yali ToolKit", "yaliToolKit"),
    ],
)
def test_str_to_camelcase(input, output):
    assert StringConv.to_camelcase(input) == output


@pytest.mark.parametrize(
    "input, output",
    [
        # With punctuation.
        ("Yali, toolkit!", "YaliToolkit"),
        # Camel cased
        ("yaliToolkit", "YaliToolkit"),
        # Joined by delimeter.
        ("Yali-Toolkit", "YaliToolkit"),
        # Cobol cased
        ("YALI-TOOLKIT", "YaliToolkit"),
        # Without punctuation.
        ("Yali toolkit", "YaliToolkit"),
        # Repeating single delimeter
        ("Yali   Toolkit", "YaliToolkit"),
        # Repeating delimeters of different types
        ("Yali -__  Toolkit", "YaliToolkit"),
        # Wrapped in delimeter
        (" yali toolkit ", "YaliToolkit"),
        # End in capital letter
        ("yalI", "YalI"),
        # Long sentence with punctuation
        (
            r"all work !nO play makes JacK @a duLl Gu'y",
            "AllWorkNoPlayMakesJacKADuLlGuy",
        ),
        # Alternating character cases
        ("yali ToolKit", "YaliToolKit"),
    ],
)
def test_str_to_pascalcase(input, output):
    assert StringConv.to_pascalcase(input) == output


@pytest.mark.parametrize(
    "input, output",
    [
        # With punctuation.
        ("Yali, toolkit!", "YALI-TOOLKIT"),
        # Camel cased
        ("yaliToolkit", "YALI-TOOLKIT"),
        # Joined by delimeter.
        ("Yali-Toolkit", "YALI-TOOLKIT"),
        # Cobol cased
        ("YALI-TOOLKIT", "YALI-TOOLKIT"),
        # Without punctuation.
        ("Yali toolkit", "YALI-TOOLKIT"),
        # Repeating single delimeter
        ("Yali   Toolkit", "YALI-TOOLKIT"),
        # Repeating delimeters of different types
        ("Yali -__  Toolkit", "YALI-TOOLKIT"),
        # Wrapped in delimeter
        (" yali toolkit ", "YALI-TOOLKIT"),
        # End in capital letter
        ("yalI", "YAL-I"),
        # Long sentence with punctuation
        (
            r"all work !nO play makes JaCK @a duLl Gu'y",
            "ALL-WORK-NO-PLAY-MAKES-JA-CK-A-DU-LL-GUY",
        ),
        # Alternating character cases
        ("yali ToolKit", "YALI-TOOL-KIT"),
    ],
)
def test_str_to_cobolcase(input, output):
    assert StringConv.to_cobolcase(input) == output


@pytest.mark.parametrize(
    "input, output",
    [
        # With punctuation.
        ("Yali, toolkit!", "YALI_TOOLKIT"),
        # Camel cased
        ("yaliToolkit", "YALI_TOOLKIT"),
        # Joined by delimeter.
        ("Yali-Toolkit", "YALI_TOOLKIT"),
        # Cobol cased
        ("YALI-TOOLKIT", "YALI_TOOLKIT"),
        # Without punctuation.
        ("Yali toolkit", "YALI_TOOLKIT"),
        # Repeating single delimeter
        ("Yali   Toolkit", "YALI_TOOLKIT"),
        # Repeating delimeters of different types
        ("Yali -__  Toolkit", "YALI_TOOLKIT"),
        # Wrapped in delimeter
        (" yali toolkit ", "YALI_TOOLKIT"),
        # End in capital letter
        ("yalI", "YAL_I"),
        # Long sentence with punctuation
        (
            r"all work !nO play makes JaCK @a duLl Gu'y",
            "ALL_WORK_NO_PLAY_MAKES_JA_C_K_A_DU_LL_GUY",
        ),
        # Alternating character cases
        ("yali ToolKit", "YALI_TOOL_KIT"),
    ],
)
def test_str_to_macrocase(input, output):
    assert StringConv.to_macrocase(input) == output


@pytest.mark.parametrize(
    "input, output",
    [
        # With punctuation.
        ("Yali, toolkit!", "yali-toolkit"),
        # Camel cased
        ("yaliToolkit", "yali-toolkit"),
        # Joined by delimeter.
        ("Yali-Toolkit", "yali-toolkit"),
        # Cobol cased
        ("YALI-TOOLKIT", "yali-toolkit"),
        # Without punctuation.
        ("Yali toolkit", "yali-toolkit"),
        # Repeating single delimeter
        ("Yali   Toolkit", "yali-toolkit"),
        # Repeating delimeters of different types
        ("Yali -__  Toolkit", "yali-toolkit"),
        # Wrapped in delimeter
        (" yali toolkit ", "yali-toolkit"),
        # End in capital letter
        ("yalI", "yal-i"),
        # Long sentence with punctuation
        (
            r"all work !nO play makes JACK @a duLl Gu'y",
            "all-work-no-play-makes-jack-a-du-ll-guy",
        ),
        # Alternating character cases
        ("yali ToolKit", "yali-tool-kit"),
    ],
)
def test_str_to_kebabcase(input, output):
    assert StringConv.to_kebabcase(input) == output


@pytest.mark.parametrize(
    "input, output",
    [
        # With punctuation.
        ("Yali, toolkit!", "yali_toolkit"),
        # Camel cased
        ("yaliToolkit", "yali_toolkit"),
        # Joined by delimeter.
        ("Yali-Toolkit", "yali_toolkit"),
        # Cobol cased
        ("YALI-TOOLKIT", "yali_toolkit"),
        # Without punctuation.
        ("Yali toolkit", "yali_toolkit"),
        # Repeating single delimeter
        ("Yali   Toolkit", "yali_toolkit"),
        # Repeating delimeters of different types
        ("Yali -__  Toolkit", "yali_toolkit"),
        # Wrapped in delimeter
        (" yali toolkit ", "yali_toolkit"),
        # End in capital letter
        ("yalI", "yal_i"),
        # Long sentence with punctuation
        (
            r"all work !nO play makes JACK @a duLl Gu'y",
            "all_work_no_play_makes_jack_a_du_ll_guy",
        ),
        # Alternating character cases
        ("yali ToolKit", "yali_tool_kit"),
    ],
)
def test_str_to_snakecase(input, output):
    assert StringConv.to_snakecase(input) == output


@pytest.mark.parametrize(
    "input, output",
    [
        # With punctuation.
        ("Yali, toolkit!", "yalitoolkit"),
        # Camel cased
        ("yaliToolkit", "yalitoolkit"),
        # Joined by delimeter.
        ("Yali-Toolkit", "yalitoolkit"),
        # Cobol cased
        ("YALI-TOOLKIT", "yalitoolkit"),
        # Without punctuation.
        ("Yali toolkit", "yalitoolkit"),
        # Repeating single delimeter
        ("Yali   Toolkit", "yalitoolkit"),
        # Repeating delimeters of different types
        ("Yali -__  Toolkit", "yalitoolkit"),
        # Wrapped in delimeter
        (" yali toolkit ", "yalitoolkit"),
        # End in capital letter
        ("yalI", "yali"),
        # Long sentence with punctuation
        (
            r"all work !nO play makes JACK @a duLl Gu'y",
            "allworknoplaymakesjackadullguy",
        ),
        # Alternating character cases
        ("yali ToolKit", "yalitoolkit"),
    ],
)
def test_str_flatlower(input, output):
    assert StringConv.to_flatlower(input) == output


@pytest.mark.parametrize(
    "input, output",
    [
        # With punctuation.
        ("Yali, toolkit!", "YALITOOLKIT"),
        # Camel cased
        ("yaliToolkit", "YALITOOLKIT"),
        # Joined by delimeter.
        ("Yali-Toolkit", "YALITOOLKIT"),
        # Cobol cased
        ("YALI-TOOLKIT", "YALITOOLKIT"),
        # Without punctuation.
        ("Yali toolkit", "YALITOOLKIT"),
        # Repeating single delimeter
        ("Yali   Toolkit", "YALITOOLKIT"),
        # Repeating delimeters of different types
        ("Yali -__  Toolkit", "YALITOOLKIT"),
        # Wrapped in delimeter
        (" yali toolkit ", "YALITOOLKIT"),
        # End in capital letter
        ("yalI", "YALI"),
        # Long sentence with punctuation
        (
            r"all work !nO play makes JACK @a duLl Gu'y",
            "ALLWORKNOPLAYMAKESJACKADULLGUY",
        ),
        # Alternating character cases
        ("yali ToolKit", "YALITOOLKIT"),
    ],
)
def test_str_flatupper(input, output):
    assert StringConv.to_flatupper(input) == output
