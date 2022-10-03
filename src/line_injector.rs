use std::{collections::VecDeque, fmt::Debug};

#[derive(Debug)]
pub struct Injection<T> {
    line_nums: VecDeque<usize>,
    lines: VecDeque<T>,
    next: Option<(usize, T)>,
}

impl<T> Injection<T> {
    pub fn new(mut numbered_lines: Vec<(usize, T)>) -> Self {
        // Sort rows
        numbered_lines.sort_by_key(|(line_num, _line)| *line_num);

        // Extract sorted row_nums & data
        let mut line_nums = VecDeque::new();
        let mut lines = VecDeque::new();
        for (line_num, line) in numbered_lines {
            line_nums.push_back(line_num);
            lines.push_back(line);
        }

        let mut new = Self {
            line_nums,
            lines,
            next: None,
        };

        // Prepare the first line for injection
        new.prepare();
        return new;
    }

    pub fn next_line_num(&self) -> Option<usize> {
        self.next.as_ref().map(|(line_num, _line)| *line_num)
    }

    pub fn pop_next_line(&mut self) -> Option<T> {
        self.next.take().map(|(_line_num, line)| {
            self.prepare();
            line
        })
    }

    /// Prepare the next line for injection.
    /// If a new line is successfully prepared, return `true`.
    /// Otherwise, return `false`.
    fn prepare(&mut self) -> bool {
        match (self.line_nums.pop_front(), self.lines.pop_front()) {
            (Some(line_num), Some(line)) => {
                self.next = Some((line_num, line));
                true
            }
            (None, None) => false,
            _ => panic!("different number of lines & line numbers!"),
        }
    }
}

/// Combine two sets of lines into a single text file,
/// injecting (overwriting) one onto the other at specified
/// line numbers, padding with spaces if necessary.
#[derive(Debug)]
pub struct LineInjector<T, E, I: Iterator<Item = Result<T, E>>> {
    base: I,
    injection: Injection<T>,
    line_num: usize,
}

impl<T: Debug + Default, E: Debug, I: Iterator<Item = Result<T, E>>> LineInjector<T, E, I> {
    pub fn new(base: I, injection: Injection<T>) -> Self {
        Self {
            base,
            injection,
            line_num: 0,
        }
    }

    fn increment_line_num(&mut self) {
        self.line_num += 1;
    }

    fn next_inner(&mut self) -> Option<Result<T, E>> {
        match (self.base.next(), self.injection.next_line_num()) {
            (None, None) => None,
            (None, Some(next_inject_line_num)) => {
                if self.line_num == next_inject_line_num {
                    Ok(self.injection.pop_next_line()).transpose()
                } else {
                    Some(Ok(T::default()))
                }
            }
            (Some(base_line), None) => Some(base_line),
            (Some(base_line), Some(next_inject_line_num)) => {
                if self.line_num == next_inject_line_num {
                    Ok(self.injection.pop_next_line()).transpose()
                } else {
                    Some(base_line)
                }
            }
        }
    }
}

impl<T: Default + Debug, E: Debug, I: Iterator<Item = Result<T, E>>> Iterator
    for LineInjector<T, E, I>
{
    type Item = Result<T, E>;

    fn next(&mut self) -> Option<Self::Item> {
        let val = self.next_inner();
        self.increment_line_num();

        val
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_line_injector() {
        let base_lines = vec![
            "the first line",
            "the second line",
            "the third line",
            "the fourth line",
            "the fifth line",
        ];

        let inject_lines = vec![(3, "NEW 3"), (4, "NEW 4"), (10, "NEW 10")];

        let expected_lines = vec![
            "the first line",
            "the second line",
            "the third line",
            "NEW 3",
            "NEW 4",
            "",
            "",
            "",
            "",
            "",
            "NEW 10",
        ];

        let base_iter = base_lines.into_iter().map(Ok);

        let inject_iter = inject_lines.into_iter();
        let inject_vec: Vec<_> = inject_iter.collect();

        let injection = Injection::new(inject_vec);
        let injector = LineInjector::<_, anyhow::Error, _>::new(base_iter, injection);

        let collected_lines: Vec<_> = injector
            .collect::<anyhow::Result<_>>()
            .expect("not all success");

        assert_eq!(collected_lines, expected_lines)
    }
}
