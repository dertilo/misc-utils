try:
    from pydantic import BaseModel, root_validator, ValidationError

    class MustHaveSameLen(BaseModel):
        text: str
        ints: list[int]

        @root_validator
        def validate(cls, values):
            have_same_len = len(values["text"]) == len(values["ints"])
            assert have_same_len
            return values

    def test_MustHaveSameLen():
        try:
            MustHaveSameLen(text="hello", ints=list(range(4)))
        except ValidationError as e:
            print(e)


except Exception as e:
    pass  # who needs pydantic!
