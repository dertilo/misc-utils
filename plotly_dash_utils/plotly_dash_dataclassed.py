from dataclasses import dataclass
from typing import Any

import dash
import dash_core_components as dcc
import dash_html_components as html
from beartype.roar import BeartypeCallHintPepParamException
from dash import Dash
from dash.dependencies import Input, Output, State

from misc_utils.dataclass_utils import serialize_dataclass, deserialize_dataclass
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
                o = fun(
                    *[deserialize(s) for s in args],
                    **{k: deserialize(s) for k, s in kwargs.items()},
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

error = None
try:
    deserialize_dataclass(None)
except BeartypeCallHintPepParamException as e:
    error = e
assert error is not None


@dataclass
class SimpleDataClass:
    count: int = 0


def deserialize(x: Any):
    if x is None:
        return x
    elif isinstance(x, str):
        return just_try(lambda: deserialize_dataclass(x), default=x)
    else:
        print(f"deserialize: {type(x)=},{x=}")
        return x


@app.callback_dataclassed(
    Output(component_id="my-output", component_property="children"),
    Output("my-state", "data"),
    Input(component_id="my-input", component_property="value"),
    State("my-state", "data"),
)
def update_output_div(input_value, state: SimpleDataClass):
    print(f"{type(state)=}")
    if state is not None:
        count = state.count
    else:
        count = 0
    return {"dict": input_value, "state": state}, SimpleDataClass(count=count + 1)


if __name__ == "__main__":
    app.run_server(debug=True)
