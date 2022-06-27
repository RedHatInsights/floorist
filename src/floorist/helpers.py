from datetime import date


def generate_name(bucket_name, prefix=None):

    file_name = date.today().strftime('year_created=%Y/month_created=%-m/day_created=%-d')
    parts = ["s3:/", bucket_name, file_name]
    if prefix:
        parts.insert(2, prefix)

    return '/'.join(parts)

def validate_floorplan_entry(query, prefix):
    if not query:
        raise ValueError("Query cannot be empty!")
    elif not prefix:
        raise ValueError("Prefix cannot be empty!")
    else:
        return True
