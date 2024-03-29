use headers::{Header, HeaderName, HeaderValue};

#[derive(Debug)]
pub struct ContentRange {
    pub start: u64,
    pub end: u64,
}

static CONTENT_RANGE_NAME: HeaderName = HeaderName::from_static("content-range");

impl Header for ContentRange {
    fn name() -> &'static HeaderName {
        &CONTENT_RANGE_NAME
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
    where
        I: Iterator<Item = &'i HeaderValue>,
    {
        let value = values.next().ok_or_else(headers::Error::invalid)?;
        let s = value.to_str().map_err(|_| headers::Error::invalid())?;
        let ss = s
            .split('-')
            .map(|s| s.parse::<u64>())
            .collect::<Result<Vec<u64>, _>>()
            .map_err(|_| headers::Error::invalid())?;

        if ss.len() < 2 {
            return Err(headers::Error::invalid());
        } else if ss.len() > 2 {
            return Err(headers::Error::invalid());
        }

        let start = ss[0];
        let end = ss[1];

        Ok(ContentRange { start, end })
    }

    fn encode<E>(&self, values: &mut E)
    where
        E: Extend<HeaderValue>,
    {
        let value = HeaderValue::from_str(&format!("{}-{}", self.start, self.end))
            .expect("this should always work");
        values.extend(std::iter::once(value))
    }
}

#[derive(Debug)]
pub struct Range {
    pub start: u64,
    pub end: u64,
}

impl From<ContentRange> for Range {
    fn from(cr: ContentRange) -> Range {
        Range {
            start: cr.start,
            end: cr.end,
        }
    }
}

impl Into<String> for &Range {
    fn into(self) -> String {
        format!("{}-{}", self.start, self.end)
    }
}

static RANGE_NAME: HeaderName = HeaderName::from_static("range");

impl Header for Range {
    fn name() -> &'static HeaderName {
        &RANGE_NAME
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
    where
        I: Iterator<Item = &'i HeaderValue>,
    {
        let value = values.next().ok_or_else(headers::Error::invalid)?;
        let s = value.to_str().map_err(|_| headers::Error::invalid())?;
        let ss = s
            .split('-')
            .map(|s| s.parse::<u64>())
            .collect::<Result<Vec<u64>, _>>()
            .map_err(|_| headers::Error::invalid())?;

        if ss.len() < 2 {
            return Err(headers::Error::invalid());
        } else if ss.len() > 2 {
            return Err(headers::Error::invalid());
        }

        let start = ss[0];
        let end = ss[1];

        Ok(Range { start, end })
    }

    fn encode<E>(&self, values: &mut E)
    where
        E: Extend<HeaderValue>,
    {
        let value = HeaderValue::from_str(&format!("{}-{}", self.start, self.end))
            .expect("this should always work");
        values.extend(std::iter::once(value))
    }
}
