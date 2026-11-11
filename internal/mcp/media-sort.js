const mediaTable = document.querySelector("main table");

if (mediaTable) {
  const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: "base" });
  const headings = mediaTable.tHead.rows[0].cells;
  const body = mediaTable.tBodies[0];

  for (const heading of headings) {
    heading.querySelector("button").addEventListener("click", () => {
      const ascending = heading.getAttribute("aria-sort") !== "ascending";
      const numeric = heading.hasAttribute("data-numeric");
      const rows = Array.from(body.rows, (row) => {
        const cell = row.cells[heading.cellIndex];
        return { row, value: cell.dataset.sort ?? cell.textContent.trim() };
      });

      rows.sort((a, b) => {
        const order = numeric ? Number(a.value) - Number(b.value) : collator.compare(a.value, b.value);
        return ascending ? order : -order;
      });

      const sorted = document.createDocumentFragment();
      for (const { row } of rows) sorted.append(row);
      body.append(sorted);
      for (const other of headings) other.removeAttribute("aria-sort");
      heading.setAttribute("aria-sort", ascending ? "ascending" : "descending");
    });
  }
}
