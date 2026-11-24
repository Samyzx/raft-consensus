const select = (query, scope = document) => scope.querySelector(query);
const names = {standard: "Standard Raft", lease: "Lease-based Raft"};
const descriptions = {
  standard: "Fresh quorum confirmation for each linearizable read.",
  lease: "Local reads while a quorum-backed leader lease is valid.",
};
const roles = {
  leader: "Leader", follower: "Follower", candidate: "Candidate",
  stopped: "Stopped", starting: "Starting", exited: "Exited",
};
const clusterCards = new Map();
const previousRoles = new Map();
let busy = false;
let generation = -1;

function row(tag, label, className = "") {
  const element = document.createElement(tag);
  element.textContent = label;
  element.className = className;
  return element;
}

function record(event) {
  const list = select("#activity");
  list.querySelector(".empty-activity")?.remove();
  const item = document.createElement("li");
  const time = new Date().toLocaleTimeString([], {hourCycle: "h23"});
  item.append(row("time", time), row("span", event));
  list.prepend(item);
  while (list.children.length > 18) list.lastElementChild.remove();
}

async function api(path, body) {
  const options = body === undefined ? {} : {
    method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(body),
  };
  const response = await fetch(`/api/${path}`, options);
  const result = await response.json();
  if (!response.ok) throw new Error(result.error ?? `Request failed (${response.status})`);
  return result;
}

function updateButtons() {
  for (const button of document.querySelectorAll("button")) {
    button.disabled = busy || button.dataset.available === "false";
  }
}

function render(state) {
  const container = select("#clusters");
  if (generation !== state.generation) {
    generation = state.generation;
    clusterCards.clear();
    previousRoles.clear();
    container.replaceChildren();
    select("#count").value = String(state.clusters[0].nodes.length);
  }
  for (const cluster of state.clusters) {
    const {mode, nodes, majority} = cluster;
    if (!clusterCards.has(mode)) {
      const card = select("#cluster-template").content.firstElementChild.cloneNode(true);
      card.dataset.mode = mode;
      select("[data-name]", card).textContent = names[mode];
      select("[data-description]", card).textContent = descriptions[mode];
      const nodeCards = new Map();
      for (const {id, address} of nodes) {
        const element = select("#node-template").content.firstElementChild.cloneNode(true);
        element.title = address;
        select("[data-node-name]", element).textContent = `N${id}`;
        select("[data-node]", element).dataset.node = id;
        select("[data-nodes]", card).append(element);
        nodeCards.set(id, element);
      }
      container.append(card);
      clusterCards.set(mode, {card, nodeCards});
    }
    const {card, nodeCards} = clusterCards.get(mode);
    const term = Math.max(0, ...nodes.map(node => node.term ?? 0));
    const leader = nodes.find(node => node.role === "leader" && node.term === term);
    const reachable = nodes.filter(node => node.term !== undefined).length;
    select("[data-leader]", card).textContent = leader ? `N${leader.id}` : "–";
    select("[data-term]", card).textContent = term || "–";
    const live = select("[data-live]", card);
    live.textContent = `${reachable}/${nodes.length}`;
    live.classList.toggle("under-quorum", reachable < majority);
    select("[data-majority]", card).textContent = majority;
    select("[data-action=restore]", card).dataset.available = String(nodes.some(isStopped));
    for (const node of nodes) renderNode(mode, nodeCards.get(node.id), node);
    renderPolicy(mode, card, leader, reachable >= majority, state.lease_ms);
  }
  select("#compare").dataset.available = String(state.clusters.every(cluster =>
    cluster.nodes.every(node => node.term !== undefined) && cluster.nodes.some(node => node.ready),
  ));
  updateButtons();
}

function isStopped(node) {
  return node.role === "stopped" || node.role === "exited";
}

function renderNode(mode, card, node) {
  card.dataset.role = node.role;
  select("[data-role]", card).textContent = roles[node.role] ?? node.role;
  select("[data-commit]", card).textContent = node.commit_index ?? "–";
  select("[data-log]", card).textContent = node.log_index ?? "–";
  const control = select("[data-node]", card);
  const verb = isStopped(node) ? "Start" : "Stop";
  control.textContent = verb;
  control.dataset.action = verb.toLowerCase();
  control.setAttribute("aria-label", `${verb} ${names[mode]} node ${node.id}`);

  const key = `${mode}/${node.id}`;
  const former = previousRoles.get(key);
  if (node.role === "leader" && former !== `leader/${node.term}`) {
    record(`${names[mode]} · N${node.id} elected in term ${node.term}`);
  } else if (node.role === "follower" && former?.startsWith("leader/")) {
    record(`${names[mode]} · N${node.id} stepped down`);
  } else if (node.role === "candidate" && !former?.startsWith("candidate/")) {
    record(`${names[mode]} · N${node.id} started an election`);
  }
  previousRoles.set(key, `${node.role}/${node.term ?? ""}`);
}

function renderPolicy(mode, card, leader, quorum, leaseMs) {
  const policy = select("[data-policy]", card);
  const progress = select("[data-progress]", card);
  progress.hidden = true;
  if (mode === "standard") {
    policy.textContent = quorum ? "Quorum required for reads" : "Reads and writes lack a quorum";
  } else if (leader?.handoff_ms) {
    policy.textContent = `Waiting out old lease · ${(leader.handoff_ms / 1000).toFixed(1)} s`;
  } else if (leader?.lease_ms) {
    policy.textContent = `${quorum ? "Active lease" : "Quorum lost · lease expires in"} ${(leader.lease_ms / 1000).toFixed(1)} s`;
    progress.hidden = false;
    progress.max = leaseMs;
    progress.value = leader.lease_ms;
  } else {
    policy.textContent = leader ? "Waiting for a quorum-backed lease" : "No active lease";
  }
  select("[data-rounds]", card).textContent = leader ? `${leader.replication_rounds} leader rounds` : "";
}

function describe(result, verb) {
  if (!result.success) return `${result.error} (${result.elapsed_ms} ms)`;
  return `${verb === "GET" ? JSON.stringify(result.data) : "Committed"} (${result.elapsed_ms} ms)`;
}

function renderRequests(response) {
  const results = select("#client-results");
  results.hidden = false;
  results.replaceChildren(...response.results.map(result => row(
    "p", `${names[result.mode]} · ${describe(result, response.verb)}`,
    `result ${result.success ? "good" : "failure"}`,
  )));
  record(`${response.verb} ${JSON.stringify(response.key)} · ${response.results.filter(result => result.success).length}/${response.results.length} succeeded`);
}

function renderComparison(response) {
  const container = select("#scenario-results");
  const table = document.createElement("table");
  const head = table.createTHead().insertRow();
  for (const label of ["Phase", names.standard, names.lease]) {
    const heading = row("th", label);
    heading.scope = "col";
    head.append(heading);
  }
  const phases = [["Immediately after quorum loss", response.before], ["After lease leader stepped down", response.after]];
  const body = table.createTBody();
  for (const [phase, results] of phases) {
    const entry = body.insertRow();
    entry.append(row("th", phase), ...results.map(result => row(
      "td", result.success ? `Read succeeded · ${result.elapsed_ms} ms` : "Read timed out",
      result.success ? "good" : "failure",
    )));
    entry.firstElementChild.scope = "row";
  }
  const summary = response.passed
    ? "During quorum loss, only the unexpired lease could serve the committed value. Both clusters rejected reads after expiry."
    : "Observed responses differed from the expected lease behavior. Inspect the cluster state and rerun after restoring nodes.";
  container.replaceChildren(table, row("p", summary));
  container.hidden = false;
  record(`Quorum-loss experiment · ${response.passed ? "matched expected results" : "unexpected results"}`);
}

async function perform(action, body, finish) {
  if (busy) return;
  busy = true;
  updateButtons();
  try {
    const result = await api(action, body);
    if (finish) finish(result);
    else record(result.message);
  } catch (error) {
    record(error.message);
    if (action === "compare") {
      const progress = select("#scenario-progress");
      progress.textContent = error.message;
      progress.hidden = false;
    } else if (action === "request") {
      const results = select("#client-results");
      results.replaceChildren(row("p", error.message, "result failure"));
      results.hidden = false;
    }
  } finally {
    busy = false;
    updateButtons();
  }
}

select("#cluster-settings").addEventListener("submit", event => {
  event.preventDefault();
  perform("reset", {count: Number(select("#count").value)}, result => {
    select("#client-results").hidden = true;
    select("#scenario-results").hidden = true;
    select("#scenario-progress").hidden = true;
    record(result.message);
  });
});
select("#request-form").addEventListener("submit", event => {
  event.preventDefault();
  perform("request", {
    mode: select("#target").value, verb: event.submitter?.value ?? "SET",
    key: select("#key").value, value: select("#value").value,
  }, renderRequests);
});
select("#clusters").addEventListener("click", event => {
  const button = event.target.closest("button[data-action]");
  if (!button) return;
  perform(button.dataset.action, {mode: button.closest("article").dataset.mode, id: button.dataset.node});
});
select("#compare").addEventListener("click", () => {
  const progress = select("#scenario-progress");
  progress.textContent = "Running the experiment. Watch the lease countdown after the followers stop.";
  progress.hidden = false;
  select("#scenario-results").hidden = true;
  perform("compare", {}, result => {
    progress.hidden = true;
    renderComparison(result);
  });
});

async function poll() {
  const connection = select("#connection");
  try {
    const state = await api("status");
    render(state);
    connection.textContent = "Connected to local processes";
    connection.classList.add("online");
  } catch {
    connection.textContent = "Dashboard disconnected";
    connection.classList.remove("online");
  } finally {
    setTimeout(poll, 350);
  }
}

select("#compare").disabled = true;
poll();
