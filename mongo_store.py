import mongoengine


class FileRecord(mongoengine.Document):
    filename = mongoengine.StringField()
    dirpath = mongoengine.StringField()
    modified = mongoengine.FloatField()
    hash = mongoengine.StringField()
    seen = mongoengine.BooleanField()
    length = mongoengine.IntField()

class TokenPos(mongoengine.Document):
    hash = mongoengine.StringField()
    name = mongoengine.StringField()
    line = mongoengine.IntField()
    pos = mongoengine.IntField()

class Connection:

    DoesNotExist = mongoengine.DoesNotExist

    def __init__(self, dbname='test', document_class=FileRecord, create=False):
        self.conn = mongoengine.connect(dbname)
        self.document_class = document_class
        if create:
            try:
                self.document_class.drop_collection()
            except mongoengine.OperationError:
                pass

    def commit(self):
        pass

    def clear_seen_bits(self, prefix):
        self.document_class.objects(dirpath__startswith=prefix).update(seen=False)

    def hash_exists(self, hash):
        return len(FileRecord.objects(hash=hash)[:1]) == 1

    def save_reference(self, hash, name, line, pos):
        TokenPos(hash=hash, name=name, line=line, pos=pos).save()

    def id_mod_hash_seen(self, dir_path, file_path):
        fieldnames = ["id", "modified", "hash", "seen"]
        result = self.document_class.objects.only(*fieldnames).get(
            dirpath=dir_path, filename=file_path
        )
        return tuple(getattr(result, fld) for fld in fieldnames)

    def update_modified_hash_seen(self, id, modified, hash, seen=True):
        return self.document_class.objects(pk=id).update(
            modified=modified, hash=hash, seen=seen
        )

    def update_seen(self, id):
        self.document_class.objects(pk=id).update(seen=True)

    def db_insert_location(self, file_path, dirpath, modified, hash):
        rec = self.document_class(
            filename=file_path,
            dirpath=dirpath,
            modified=modified,
            hash=hash,
            seen=True,
        )
        rec.save()

    def all_file_count(self):
        return self.document_class.objects.count()

    def count_not_seen(self):
        return self.document_class.objects(seen=False).count()

    def dir_files_not_seen(self):
        fieldnames = ["dirpath", "filename"]
        result = self.document_class.objects(seen=False).only(*fieldnames)
        return [tuple(getattr(rec, fld) for fld in fieldnames) for rec in result]

    def delete_not_seen(self):
        self.document_class.objects(seen=False).delete()


if __name__ == '__main__':
    print("Have you run this code's tests?")
