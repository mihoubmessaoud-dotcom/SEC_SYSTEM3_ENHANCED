from modules.data_repository import DataRepository, DuplicateWriteError


def main() -> None:
    repo = DataRepository()

    repo.set_raw("revenue:2025", 416_161)
    repo.set_clean("gross_margin:2025", 0.462, reason="validated_from_source")

    print("RAW:", repo.get_raw("revenue:2025"))
    print("CLEAN:", repo.get_clean("gross_margin:2025"))
    print("MISSING:", repo.get_clean("roic:2025"))
    print("AUDIT_LOG:", repo.audit_log)

    try:
        repo.set_clean("gross_margin:2025", 0.470, reason="recompute")
    except DuplicateWriteError as exc:
        print("DUPLICATE_WRITE_BLOCKED:", str(exc))


if __name__ == "__main__":
    main()
