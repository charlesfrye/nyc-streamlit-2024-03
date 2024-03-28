import modal

image = modal.Image.debian_slim(python_version="3.11").pip_install(
    "requests~=2.31.0",
)

stub = modal.Stub(
    "coffeeshop-db-client",
    image=image,
)
