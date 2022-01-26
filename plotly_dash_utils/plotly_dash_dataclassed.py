from dataclasses import dataclass
from typing import Any, Optional

import dash_core_components as dcc
import dash_html_components as html
from beartype import beartype
from dash import Dash
from dash.dependencies import Input, Output, State

from misc_utils.beartypes import bear_does_roar
from misc_utils.dataclass_utils import serialize_dataclass, deserialize_dataclass, \
    encode_dataclass
from misc_utils.utils import just_try


class DashDataclasses(Dash):
    def callback_dataclassed(self, *_args, **_kwargs):
        """
        wraps the dash-decorator and putting another decorator in front of it
        1. serialize_deserialize_dataclasses_wrapper
        2. app.callback, super().callback here
        :return:
        """

        def some_decorator(fun):
            def serialize_deserialize_dataclasses_wrapper(*args, **kwargs):
                bearified_fun = beartype(fun)
                o = bearified_fun(
                    *[just_try_deserialize_or_fallback(x) for x in args],
                    **{k:just_try_deserialize_or_fallback(x) for k,x in kwargs.items()},
                )
                if isinstance(o, (list, tuple)):
                    o = [serialize_dataclass(dc) for dc in o]
                else:
                    o = serialize_dataclass(o)
                return o

            return self.callback(*_args, **_kwargs)(
                serialize_deserialize_dataclasses_wrapper
            )

        return some_decorator


app = DashDataclasses(__name__)

app.layout = html.Div(
    [
        html.H6("Change the value in the text box to see callbacks in action!"),
        html.Div(
            ["Input: ", dcc.Input(id="my-input", value="initial value", type="text")]
        ),
        html.Br(),
        html.Div(id="my-output"),
        dcc.Store("my-state"),
    ]
)

assert bear_does_roar(lambda : encode_dataclass(None))

@dataclass
class SimpleDataClass:
    count: int = 0


def just_try_deserialize_or_fallback(x: Any):
    if x is not None:
        return just_try(lambda: deserialize_dataclass(x), default=x)
    else:
        return None

@app.callback_dataclassed(
    Output(component_id="my-output", component_property="children"),
    Output("my-state", "data"),
    Input(component_id="my-input", component_property="value"),
    State("my-state", "data"),
)
def update_output_div(
    input_value, state: Optional[SimpleDataClass]
) -> tuple[dict, SimpleDataClass]:
    print(f"{type(state)=}")
    if state is not None:
        count = state.count
    else:
        count = 0
    return {"dict": input_value, "state": state}, SimpleDataClass(count=count + 1)


if __name__ == "__main__":
    app.run_server(debug=True)
