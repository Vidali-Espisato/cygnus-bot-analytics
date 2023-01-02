class PageItem(tuple):
    def __new__(cls, url, visit_count, domain, page_load_speed, page_size,
                first_crawled_at, last_crawled_at, compliant,
                non_compliance_reason):
        return tuple.__new__(PageItem,
                             (url, visit_count, domain, page_load_speed,
                              page_size, first_crawled_at, last_crawled_at,
                              compliant, non_compliance_reason))

    @property
    def url(self):
        return self[0]

    @property
    def visit_count(self):
        return self[1]

    @property
    def domain(self):
        return self[2]

    @property
    def page_load_speed(self):
        return self[3]

    @property
    def page_size(self):
        return self[4]

    @property
    def first_crawled_at(self):
        return self[5]

    @property
    def last_crawled_at(self):
        return self[6]

    @property
    def non_compliance_reason(self):
        return self[8]

    @property
    def compliant(self):
        return self[7]

    def to_dict(self):
        return {
            "url": self[0],
            "visit_count": self[1],
            "domain": self[2],
            "page_load_speed": self[3],
            "page_size": self[4],
            "first_crawled_at": self[5],
            "last_crawled_at": self[6],
            "compliant": self[7],
            "non_compliance_reason": self[8]
        }


class DomainItem(tuple):
    def __new__(cls, date, domain, page_count, visit_count,
                avg_page_load_speed, total_page_size, compliance_count,
                non_compliance_count, non_compliance_reasons):
        return tuple.__new__(
            DomainItem,
            (date, domain, page_count, visit_count, avg_page_load_speed,
             total_page_size, compliance_count, non_compliance_count,
             non_compliance_reasons))

    @property
    def date(self):
        return self[0]

    @property
    def domain(self):
        return self[1]

    @property
    def page_count(self):
        return self[2]

    @property
    def visit_count(self):
        return self[3]

    @property
    def avg_page_load_speed(self):
        return self[4]

    @property
    def total_page_size(self):
        return self[5]

    @property
    def compliance_count(self):
        return self[6]

    @property
    def non_compliance_count(self):
        return self[7]

    @property
    def non_compliance_reasons(self):
        return self[8]

    def to_dict(self):
        return {
            "domain": self.domain,
            "date": self.date,
            "page_count": self.page_count,
            "total_page_size": self.total_page_size,
            "visit_count": self.visit_count,
            "compliance_count": self.compliance_count,
            "non_compliance_count": self.non_compliance_count,
            "avg_page_load_speed": self.avg_page_load_speed,
            "non_compliance_reasons": self.non_compliance_reasons
        }


class LogItem(tuple):
    def __new__(cls, url, timestamp, page_load_speed, page_size,
                non_compliance_reason):
        return tuple.__new__(LogItem, (url, timestamp, page_load_speed,
                                       page_size, non_compliance_reason))

    @property
    def url(self):
        return self[0]

    @property
    def timestamp(self):
        return self[1]

    @property
    def page_load_speed(self):
        return self[2]

    @property
    def page_size(self):
        return self[3]

    @property
    def non_compliance_reason(self):
        return self[4]

    @property
    def compliant(self):
        return self.non_compliance_reason is None


class ErrorItem(LogItem):
    def __new__(cls, url, timestamp, non_compliance_reason):
        return LogItem(url, timestamp, 0, 0, non_compliance_reason)


class InfoItem(LogItem):
    def __new__(cls, url, timestamp, page_load_speed, page_size):
        return LogItem(url, timestamp, page_load_speed, page_size, None)
