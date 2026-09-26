BVB Agent B Lite — Chart Sync Upgrade

Apply these changes to the current HTML:

1. MOVE CHART NEAR TOP
Move the entire "Price Structure Chart" card so it appears immediately below the 25-use counter and above the Agent B Action section.

2. EXPAND AGENT B PROMPT
Replace the PROMPT constant with:

const PROMPT=`Act as Agent B for BVB Trading Assistant. Analyze the attached Robinhood screenshot using the learned BVB framework and Agent B's aggressive short-term style. Read only information clearly visible; do not invent unreadable numbers.

Return ONLY valid JSON with these keys:
action (WAIT|ARMED|ENTER|HOLD|EXIT),
ticker,
timeframe,
currentPrice,
sma8,
sma20,
sma200,
sma200Detected,
support,
resistance,
trend,
ma,
state,
event,
momentum,
supportResistance,
trigger,
triggerPrice,
risk,
invalidationPrice,
targetPrice,
reason.

Use null for numeric values that cannot reliably be read. support and resistance may be a number or array. Prioritize 2m/5m structure; 8/20/200 SMA alignment; narrow/trending/wide state; location near the 8/20 or support/resistance; Elephant, Tail, 180, Color Change; breakout/continuation momentum; clean air; do-not-chase discipline; and fast invalidation.`;

3. ADD GLOBAL SCREENSHOT OVERLAY
Change:
let lastBars=[],lastA=null;
to:
let lastBars=[],lastA=null,shotOverlay=null;

4. ADD HELPERS
Add:

function num(v){
 if(v===null||v===undefined||v==='')return NaN;
 const n=Number(String(v).replace(/[$,]/g,''));
 return Number.isFinite(n)?n:NaN;
}
function levels(v){
 if(Array.isArray(v))return v.map(num).filter(Number.isFinite);
 const n=num(v); return Number.isFinite(n)?[n]:[];
}

5. ADD SCREENSHOT-TO-CHART SYNC
Add:

function syncScreenshot(j){
 const detected=String(j.ticker||'').trim().toUpperCase();
 const app=$('ticker').value.trim().toUpperCase();

 if(!detected){
   return 'Ticker unreadable; screenshot chart values were not synchronized.';
 }
 if(detected!==app){
   return `Screenshot is ${detected}, chart is ${app}; levels were not transferred.`;
 }

 shotOverlay={
   ticker:detected,
   timeframe:j.timeframe||'',
   currentPrice:num(j.currentPrice),
   sma8:num(j.sma8),
   sma20:num(j.sma20),
   sma200:num(j.sma200),
   support:levels(j.support),
   resistance:levels(j.resistance),
   triggerPrice:num(j.triggerPrice),
   invalidationPrice:num(j.invalidationPrice),
   targetPrice:num(j.targetPrice)
 };

 if(Number.isFinite(shotOverlay.sma200)){
   localStorage.setItem('bvb_b_200_'+detected,String(shotOverlay.sma200));
   $('manual200').value=shotOverlay.sma200;
 }

 localStorage.setItem('bvb_b_shot_'+detected,JSON.stringify(shotOverlay));

 if(lastBars.length){
   lastA=analyze(lastBars);
   draw(lastBars,lastA);
 }

 return `${detected} screenshot data synchronized${shotOverlay.timeframe?' • '+shotOverlay.timeframe:''}.`;
}

6. MODIFY applyAgent()
At the end of applyAgent(), replace the shotStatus assignment with:

const sync=syncScreenshot(j);
$('shotStatus').textContent=source+' applied. '+sync;

7. ADD CHART DATA DISPLAY
Directly above <canvas id="chart"></canvas>, add:

<div class="readgrid">
 <div class="box"><div class="label">Price</div><div id="chartPrice" class="big">—</div></div>
 <div class="box"><div class="label">8 SMA</div><div id="chart8" class="big">—</div></div>
 <div class="box"><div class="label">20 SMA</div><div id="chart20" class="big">—</div></div>
 <div class="box"><div class="label">200 SMA</div><div id="chart200" class="big">—</div></div>
 <div class="box"><div class="label">Support</div><div id="chartSupport" class="big">—</div></div>
 <div class="box"><div class="label">Resistance</div><div id="chartResistance" class="big">—</div></div>
</div>

8. ADD DISPLAY SYNC
Add:

function renderChartValues(a){
 const ov=shotOverlay&&shotOverlay.ticker===$('ticker').value.trim().toUpperCase()?shotOverlay:null;
 $('chartPrice').textContent=fmt(ov&&Number.isFinite(ov.currentPrice)?ov.currentPrice:a?.p);
 $('chart8').textContent=fmt(ov&&Number.isFinite(ov.sma8)?ov.sma8:a?.m8);
 $('chart20').textContent=fmt(ov&&Number.isFinite(ov.sma20)?ov.sma20:a?.m20);
 $('chart200').textContent=fmt(ov&&Number.isFinite(ov.sma200)?ov.sma200:a?.m200);
 $('chartSupport').textContent=fmt(ov&&ov.support.length?ov.support[0]:a?.support);
 $('chartResistance').textContent=fmt(ov&&ov.resistance.length?ov.resistance[0]:a?.resist);
}

Call renderChartValues(a); inside render(a), immediately before draw(lastBars,a).

9. DRAW SCREENSHOT LEVELS
Inside draw(), after the normal SMA lines are drawn, add:

const ov=shotOverlay&&shotOverlay.ticker===$('ticker').value.trim().toUpperCase()?shotOverlay:null;

function hline(v,color,label,dash=[]){
 if(!Number.isFinite(v))return;
 const y=Y(v);
 x.save();
 x.strokeStyle=color;
 x.fillStyle=color;
 x.setLineDash(dash);
 x.beginPath();
 x.moveTo(pad.l,y);
 x.lineTo(W-pad.r,y);
 x.stroke();
 x.setLineDash([]);
 x.font='9px -apple-system';
 x.fillText(label+' '+v.toFixed(2),W-pad.r-58,y-3);
 x.restore();
}

if(ov){
 hline(ov.sma8,'#f59e0b','8');
 hline(ov.sma20,'#f4f7fb','20');
 hline(ov.sma200,'#4a94ff','200');
 ov.support.forEach(v=>hline(v,'#42d99b','S',[5,4]));
 ov.resistance.forEach(v=>hline(v,'#ff7070','R',[5,4]));
 hline(ov.currentPrice,'#7dd3fc','PX',[2,3]);
 hline(ov.triggerPrice,'#ffd166','TRG',[3,3]);
 hline(ov.invalidationPrice,'#ff7070','INV',[3,3]);
 hline(ov.targetPrice,'#42d99b','TGT',[3,3]);
}

10. LOAD SAVED SCREENSHOT DATA WHEN TICKER CHANGES
Add:

$('ticker').addEventListener('change',()=>{
 const sym=$('ticker').value.trim().toUpperCase();
 $('ticker').value=sym;
 try{
   shotOverlay=JSON.parse(localStorage.getItem('bvb_b_shot_'+sym)||'null');
 }catch(_){shotOverlay=null}
 if(lastBars.length)render(analyze(lastBars));
});

RESULT:
- Chart is near the top.
- Matching screenshot ticker can supply 8/20/200 SMA.
- Screenshot 200 SMA is saved per stock.
- Screenshot support/resistance is displayed.
- Current price, trigger, invalidation and target can be drawn.
- Screenshot values override displayed reference levels only for the matching ticker.
- Unreadable values remain unknown instead of being invented.
