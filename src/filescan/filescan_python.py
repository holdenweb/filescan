from tokenize import tokenize
import token
import keyword as kw


EXTENSIONS = [".py", ".pyw"]


def process(conn, loc):
    """
    Add the non-keyword tokens to the position index for this file.
    Only called when no checksum previously existed for the file's
    current incarnation - otherwise we assume scanning took place
    when the original checksum was created.
    XXX The above assumption fails when the first incarnation of a
        Python source file doesn't have the ".py" extension. Hmmm.
        Maybe one solution is an explicit test for the existence of
        at least one TokenPos for a given checksum, but even this
        would cause repeated parsing of files containing no names.
    """
    filepath = f"{loc.dirpath}{loc.filename}"
    if any(filepath.endswith(ext) for ext in EXTENSIONS):
        with open(filepath, "rb") as inf:
            try:
                for t in tokenize(inf.readline):
                    if t.type == token.NAME and not kw.iskeyword(t.string):
                        conn.save_reference(
                            loc.checksum, t.string, t.start[0], t.start[1]
                        )
            except Exception as e:
                print(
                    f"** {filepath}: {type(e)}\n   {e}"
                )  # XXX: sensible handling of parse and other errors
