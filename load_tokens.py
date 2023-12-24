from tokenize import tokenize
import token
import keyword as kw
import importlib


def scan_tokens(conn, filepath, checksum):
    """
    Add the non-keywork tokens to the position index for this file.

    If any file with the same checksum has already been scanned, or if
    it isn't a Python file, take no action.
    """
    if not filepath.endswith(".py"):
        return
    cs = conn.hash_for(checksum)
    with open(filepath, "rb") as inf:
        try:
            for t in tokenize(inf.readline):
                if t.type == token.NAME and not kw.iskeyword(t.string):
                    conn.save_reference(cs, t.string, t.start[0], t.start[1])
                    assert t.start[0] == t.end[0] and t.end[1] == t.start[1] + len(
                        t.string
                    )
        except Exception as e:
            print(e)  # TODO: sensible handling of parse and other errors


if __name__ == "__main__":
    storage = "postgresql"
    database = "filescan"
    store_name = f"{storage}_store"
    print("Using", store_name)
    store = importlib.import_module(store_name)
    conn = store.Connection(database, create=True)
    scan_tokens(conn, "load_tokens.py", "bogus-checksum")
    conn.commit()
