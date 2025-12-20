# Copilot Instructions

NEVER use tools directly, if you can use them through `hatch` commands!

## Running the unit tests

To run the unit tests for the `llm_dataclass` package, you can use the following command in your terminal:

```bash
hatch test
```

## Type checking

For type checking, you can use `mypy`:

```bash
hatch run types:check
```

## Linting
To lint the code, you can use `ruff`:
```bash
hatch run ruff:ruff check
```
