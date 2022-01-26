from dataclasses import dataclass
from typing import Any

import dash
import dash_core_components as dcc
import dash_html_components as html
from beartype.roar import BeartypeCallHintPepParamException
from dash.dependencies import Input, Output, State

from misc_utils.dataclass_utils import serialize_dataclass, deserialize_dataclass
from misc_utils.utils import just_try

app = dash.Dash(__name__)

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


def app_callback_dataclassed(*args, **kwargs):
    def some_decorator(fun):
        def inner_function(*_args, **_kwargs):
            # assert all((isinstance(s,str) for s in _args))
            # assert all((isinstance(s,str) for s in _kwargs.values()))
            o = fun(
                *[deserialize(s) for s in _args],
                **{k: deserialize(s) for k, s in _kwargs.items()},
            )
            if isinstance(o, (list, tuple)):
                o = [serialize_dataclass(dc) for dc in o]
            else:
                o = serialize_dataclass(o)
            return o

        return app.callback(*args, **kwargs)(inner_function)

    return some_decorator


@app_callback_dataclassed(
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
