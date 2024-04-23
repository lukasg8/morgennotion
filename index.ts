/**
 * Script for synchronizing tasks between Notion and Morgen
 * Author: Lukas Grunzke
 * Creation Date: 11/11/2023
 * Description: This script integrates Notion API and Morgen API to sync tasks/events.
 * Dependencies: Notion API, Morgen API, Luxon for date handling
 */

import { Client } from "@notionhq/client";
import { config } from "dotenv";
import axios from 'axios';
import { PropertyItemObjectResponse } from "../../build/src/api-endpoints";
import { DateTime } from 'luxon';

config();

const notion = new Client({ auth: process.env.NOTION_KEY });
const databaseId = process.env.NOTION_DATABASE_ID!;

// Datatypes
type Task = {
  pageId:string;
  eventId:string;
  title:string;
  dueDate:string;
  description:string;
  area:string;
  status:string;
  lastUpdate:string;
}

type MorgenEvent = {
  eventId:string;
  pageId:string;
  title:string;
  description:string;
  start:string;
  duration:string;
  lastUpdate:string;
}

type UniversalTaskEvent = {
  notionPageId: string | null;
  morgenEventId: string | null;
  title: string;
  description: string;
  date: string;
  duration: string;
  lastUpdate: string;
}

type EventPair = {
  notion?: UniversalTaskEvent;
  morgen?: UniversalTaskEvent;
}

type NotionTaskCreationResponse = {
  object: string;
  id: string;
  created_time: string;
  last_edited_time: string;
  created_by: {
      object: string;
      id: string;
  };
  last_edited_by: {
      object: string;
      id: string;
  };
  cover: null | object;
  icon: null | object;
  parent: {
      type: string;
      database_id: string;
  };
  archived: boolean;
  properties: {
      [key: string]: any;
  };
  url: string;
  public_url: string | null;
  request_id: string;
};

/**
 * Converts a Notion task object to a universal task/event format.
 * @param {Task} task - The Notion task object.
 * @returns {UniversalTaskEvent} - A universal task/event object.
 */
function convertNotionToUniversal(task: Task): UniversalTaskEvent {
  return {
    notionPageId: task.pageId,
    morgenEventId: task.eventId,
    title: task.title,
    description: task.description,
    date: task.dueDate,
    duration: "PT1H",
    lastUpdate: task.lastUpdate,
  };
}

/**
 * Converts a Morgen event object to a universal task/event format.
 * @param {MorgenEvent} event - The Morgen event object.
 * @returns {UniversalTaskEvent} - A universal task/event object.
 */
function convertMorgenToUniversal(event: MorgenEvent): UniversalTaskEvent {
  return {
    notionPageId: event.pageId,
    morgenEventId: event.eventId,
    title: event.title,
    description: event.description,
    date: event.start,
    duration: event.duration,
    lastUpdate: event.lastUpdate,
  };
}

let oldUniversalEventMap: { [key: string]: EventPair } = {};
main()
setInterval(main, 40000);

/**
 * The main function of the script. It manages the synchronization process between Notion and Morgen.
 * It fetches tasks from Notion and events from Morgen, converts them to a unified format,
 * combines these events, and then synchronizes them across both platforms.
 * This function is scheduled to run periodically.
 */
async function main() {
  const rangeDays = 3;
  const yesterday = DateTime.now().setZone("America/Chicago").minus({ days: 1 }).startOf("day");
  const latestDate = yesterday.plus({ days: rangeDays });

  const notionTasks = await getTasksFromNotionDatabase(yesterday, latestDate);
  const morgenEvents = await getEventsFromMorgenAPI(yesterday, latestDate);
  const notionUniversalTasks = notionTasks.map(convertNotionToUniversal);
  const morgenUniversalEvents = morgenEvents.map(convertMorgenToUniversal);

  const newUniversalEventMap: { [key: string]: EventPair } = {};

  processAndCombineEvents(notionUniversalTasks, morgenUniversalEvents, newUniversalEventMap);
  await synchronizeEvents(oldUniversalEventMap, newUniversalEventMap);

  oldUniversalEventMap = newUniversalEventMap;
}

/**
 * Processes and combines events from Notion and Morgen into a unified event map.
 * This function iterates through both sets of events, converts them to a common format,
 * and then combines them into a single map for further processing.
 *
 * @param {UniversalTaskEvent[]} notionEvents - Array of task events from Notion.
 * @param {UniversalTaskEvent[]} morgenEvents - Array of events from Morgen.
 * @param {{ [key: string]: EventPair }} eventMap - A map object to store the combined events.
 */
function processAndCombineEvents(notionEvents: UniversalTaskEvent[], morgenEvents: UniversalTaskEvent[], eventMap: { [key: string]: EventPair }) {
  notionEvents.forEach(event => {
    const eventPair:EventPair = {
      notion: event
    }
    const key = generateEventPairKey(eventPair);
    if (eventMap[key]) {
      console.log("ERROR: Two duplicate Notion pages")
    } else {
      eventMap[key] = eventPair;
    }
  });

  morgenEvents.forEach(event => {
    const key = generateEventKey(event);
    if (eventMap[key]) {
      eventMap[key].morgen = event
    } else {
      const eventPair:EventPair = {
        morgen: event
      }
      eventMap[key] = eventPair
    }
  });
}

/**
 * Synchronizes events between Notion and Morgen based on the new and old event maps.
 * This function handles various cases such as new event creation, updates, and deletions.
 * It ensures that both Notion and Morgen have the latest and consistent event information.
 *
 * @param {{ [key: string]: EventPair }} oldMap - The previous map of events, representing the last known state.
 * @param {{ [key: string]: EventPair }} newMap - The current map of events, representing the latest state.
 */
async function synchronizeEvents(oldMap: { [key: string]: EventPair },newMap: { [key: string]: EventPair }) {

  for (const key in newMap) {
    const newEventPair = newMap[key];
    const oldEventPair = oldMap[key];

    // case 1: event exist on both platforms and has not changed
    if (oldEventPair && eventPairsAreEqual(oldEventPair, newEventPair)) {
      continue;
    }

    // case 2: event has been updated on one of the platforms
    if (oldEventPair && !eventPairsAreEqual(oldEventPair, newEventPair)) {
      await resolveAndUpdateDiscrepancies(newEventPair);
    }
    
    // case 3: event does not exist on old map (event created)
    if (!oldEventPair) {
      if (newEventPair.morgen && !newEventPair.notion) {
        await createNotionTaskFromEvent(newEventPair, newMap)
      } else if (newEventPair.notion && !newEventPair.morgen) {
        await createMorgenEventFromTask(newEventPair.notion, newMap)
      } else {
        // this is the first run (there is no oldMap)
      }
    }

    // case 4: deletions
    for (const key in oldMap) {
      const oldEventPair = oldMap[key];
      const newEventPair = newMap[key];
    
      // if newEventPair does not exist, task/event does not exist on either platform (don't need to do anything)
      if (newEventPair) {
        // if the event/task exists in the new map but only on one platform, delete from other platform
        if (newEventPair.notion && !newEventPair.morgen) {
          if (oldEventPair.morgen) {
            await deleteNotionTask(newEventPair.notion);
          }
        }
        if (newEventPair.morgen && !newEventPair.notion) {
          if (oldEventPair.notion) {
            await deleteMorgenEvent(newEventPair.morgen);
          }
        }
      }
    }
  }
}

/**
 * Resolves discrepancies and updates tasks/events across Notion and Morgen.
 * This function compares the last update timestamps for the given event pairs and
 * decides which platform (Notion or Morgen) needs updating to ensure both have the latest information.
 * It then calls the appropriate update function based on the comparison.
 *
 * @param {EventPair} eventPair - An object containing both the Notion and Morgen representations of an event.
 */
async function resolveAndUpdateDiscrepancies(eventPair:EventPair) {
  if (eventPair.morgen) {
    if (eventPair.notion) {
      if (eventPair.morgen.lastUpdate > eventPair.notion.lastUpdate) {
        await updateNotionTask(eventPair.morgen, eventPair.notion)
      } else {
        await updateMorgenEvent(eventPair.notion, eventPair.morgen)
      }
    }
  }
}

// Delete Event/Task functions
/**
 * Deletes a task from Notion by marking it as archived.
 * This function is called when a corresponding event in Morgen has been deleted or no longer needs to be synchronized.
 * It attempts to update the 'archived' status of the task in Notion.
 *
 * @param {UniversalTaskEvent} notionTask - The task event from Notion that needs to be deleted (archived).
 */
async function deleteNotionTask(notionTask:UniversalTaskEvent) {
  if (notionTask.notionPageId) {
    try {
      const response = await notion.pages.update({
        page_id: notionTask.notionPageId,
        archived: true,
      });
      console.log('Notion task updated succesfully. ', notionTask.title)
    } catch (error) {
      console.error('Error updating Notion task:', error)
    }
  }
}

/**
 * Deletes an event from Morgen.
 * This function is called when a corresponding task in Notion has been deleted or no longer needs to be synchronized.
 * It sends a request to the Morgen API to delete the specified event.
 *
 * @param {UniversalTaskEvent} morgenEvent - The event from Morgen that needs to be deleted.
 */
async function deleteMorgenEvent(morgenEvent:UniversalTaskEvent) {
  const UPDATE_MODE = "single";
  const API_URL = `https://api.morgen.so/v3/events/delete?seriesUpdateMode=${UPDATE_MODE}`;

  try {
    const response = await axios.post(API_URL, {
      accountId: process.env.MORGEN_ACCOUNT_ID,
      calendarId: process.env.MORGEN_CALENDAR_ID,
      id: morgenEvent.morgenEventId,
    }, {
      headers: {
        "Content-Type": "application/json",
        "accept": "application/json",
        "Authorization": `ApiKey ${process.env.MORGEN_API_KEY}`
      }
    });

    console.log('Morgen event deleted successfully:', morgenEvent.title);
  } catch (error) {
    console.error('Error deleting Morgen event:', error.response?.data || error.message);
  }
}

// Comparison functions
/**
 * Compares two event pairs to determine if they are equal.
 * Equality is defined as having no discrepancies in either the Morgen or Notion events
 * within each pair. It relies on the `compareEvents` function to assess equality of individual events.
 *
 * @param {EventPair} pair1 - The first event pair to compare.
 * @param {EventPair} pair2 - The second event pair to compare.
 * @returns {boolean} - Returns true if both event pairs are equal, false otherwise.
 */
function eventPairsAreEqual(pair1: EventPair, pair2: EventPair): boolean {
  if (pair1.morgen && pair1.notion && pair2.morgen && pair2.notion) {
    return !compareEvents(pair1.morgen, pair2.morgen) && !compareEvents(pair1.notion, pair2.notion)
  }
  return false
}

/**
 * Compares two events to find any discrepancies between them.
 * Discrepancies are checked in terms of title, description, and date (including time if present).
 * The function logs any discrepancies found for debugging purposes.
 *
 * @param {UniversalTaskEvent} truthEvent - The event considered as the source of truth.
 * @param {UniversalTaskEvent} compareEvent - The event to compare against the truth.
 * @returns {boolean} - Returns true if there are discrepancies, false if the events are identical.
 */
function compareEvents(truthEvent: UniversalTaskEvent, compareEvent: UniversalTaskEvent): boolean {
  let discrepancies = false;

  // Compare titles
  if (truthEvent.title !== compareEvent.title) {
    console.log(`Discrepancy found in title for "${truthEvent.title || compareEvent.title}":`,
      `Truth title: "${truthEvent.title}", Compare title: "${compareEvent.title}"`);
    discrepancies = true;
  }

  // Compare descriptions
  const truthDescription = truthEvent.description.trim();
  const compareDescription = compareEvent.description.trim();
  if (truthDescription !== compareDescription) {
    console.log(`Discrepancy found in description for "${truthEvent.title || compareEvent.title}":`,
      `Truth description: "${truthDescription}", Compare description: "${compareDescription}"`);
    discrepancies = true;
  }

  // Compare dates
  const truthDate = truthEvent.date.split('T')[0];
  const compareDate = compareEvent.date.split('T')[0];
  if (truthDate !== compareDate) {
    console.log(`Discrepancy found in date for "${truthEvent.title || compareEvent.title}":`,
      `Truth date: "${truthDate}", Compare date: "${compareDate}"`);
    discrepancies = true;
  }

  const truthHasTime = truthEvent.date.includes('T');
  const compareHasTime = compareEvent.date.includes('T');
  if (truthHasTime && compareHasTime && truthEvent.date !== compareEvent.date) {
    console.log(`Discrepancy found in time for "${truthEvent.title || compareEvent.title}":`,
      `Truth datetime: "${truthEvent.date}", Compare datetime: "${compareEvent.date}"`);
    discrepancies = true;
  }

  return discrepancies;
}

// Update Task/Event functions
/**
 * Updates an event in Morgen using data from a corresponding truth event (typically from Notion).
 * It constructs a request body with the updated information and sends a request to the Morgen API.
 *
 * @param {UniversalTaskEvent} truthEvent - The event containing the updated information.
 * @param {UniversalTaskEvent} morgenEvent - The Morgen event that needs to be updated.
 */
async function updateMorgenEvent(truthEvent: UniversalTaskEvent, morgenEvent: UniversalTaskEvent) {
  const UPDATE_MODE = "single";
  const API_URL = "https://api.morgen.so/v3/events/update?seriesUpdateMode=" + UPDATE_MODE;

  const hasTime = truthEvent.date.includes('T');
  const idPlusDescription = truthEvent.notionPageId ? `#PAGEID:${truthEvent.notionPageId}#${truthEvent.description}`: truthEvent.description;
  
  const requestBody = {
      accountId: process.env.MORGEN_ACCOUNT_ID,
      calendarId: process.env.MORGEN_CALENDAR_ID,
      id: morgenEvent.morgenEventId,
      title: truthEvent.title,
      description: idPlusDescription,
      start: formatToMorgenDateTime(truthEvent.date),
      duration: truthEvent.duration,
      timeZone: "utc",
      showWithoutTime: !hasTime,
  };

  try {
    const response = await axios.post(API_URL, requestBody, {
      headers: {
        "Content-Type": "application/json",
        "accept": "application/json",
        "Authorization": `ApiKey ${process.env.MORGEN_API_KEY}`
      }
    });
    console.log('Morgen event updated succesfully:')
  } catch (error) {
    console.log('Complete error response:', error.response || error);
  }
}

/**
 * Updates a task in Notion using data from a corresponding truth event (typically from Morgen).
 * It constructs a request body with the updated information and sends a request to the Notion API.
 *
 * @param {UniversalTaskEvent} truthEvent - The event containing the updated information.
 * @param {UniversalTaskEvent} notionEvent - The Notion task that needs to be updated.
 */
async function updateNotionTask(truthEvent: UniversalTaskEvent, notionEvent: UniversalTaskEvent) {
  if (notionEvent.notionPageId) {
    try {
      const response = await notion.pages.update({
        page_id: notionEvent.notionPageId,
        properties: {
          'Name': {
            title: [{
                text: {
                    content: truthEvent.title
                }
            }]
          },
          'Description': {
              rich_text: [{
                  text: {
                      content: truthEvent.description
                  }
              }]
          },
          'Due date': {
              date: {
                  start: truthEvent.date
              }
          },
        }
      });
      console.log('Notion task updated succesfully:')
    } catch (error) {
      console.error('Error updating Notion task:', error)
    }
  }
}

// Create Task/Event functions
/**
 * Creates a new task in Notion from a Morgen event.
 * This function converts a Morgen event into a format suitable for Notion and creates a new task.
 * It also updates the event mapping to associate the newly created Notion task with the Morgen event.
 *
 * @param {EventPair} eventPair - The pair containing the Morgen event data.
 * @param {{ [key: string]: EventPair }} newMap - The map to update with the new Notion task.
 */
async function createNotionTaskFromEvent(eventPair: EventPair, newMap: { [key: string]: EventPair }) {
  if (eventPair.morgen) {
    const morgen = eventPair.morgen
    try {
      const rawResponse = await notion.pages.create({
          "parent": {
              "type": "database_id",
              "database_id": databaseId
          },
          "properties": {
              "Name": {
                  "title": [{
                      "text": {
                          "content": morgen.title
                      }
                  }]
              },
              "Description": {
                  "rich_text": [{
                      "text": {
                          "content": morgen.description
                      }
                  }]
              },
              "Due date": {
                  "date": {
                      "start": morgen.date
                  }
              },
              "Area": {
                "select": {
                  "name": "School" // This can be improved upon (right now just set it to default School Area)
                }
              },
              "Status": {
                "status": {
                  "name": "Not started"
                }
              },
              "Morgen Event ID": {
                "rich_text": [{
                  "text": {
                    "content": morgen.morgenEventId ?? ''
                  }
                }]
              }
          },
          "children": [
              {
                  "object": "block",
                  "paragraph": {
                      "rich_text": [{
                          "text": {
                              "content": "This task was synced from Morgen.",
                          }
                      }],
                      "color": "default"
                  }
              }
          ]
      });

      const response = rawResponse as NotionTaskCreationResponse;

      console.log('Notion task created successfully: ', morgen.title);

      const onlyMorgenKey = generateEventPairKey(eventPair);
      delete newMap[onlyMorgenKey];

      morgen.notionPageId = response.id;
      await updateMorgenEventWithNotionPageId(morgen.notionPageId, morgen.morgenEventId, morgen.description);

      eventPair.notion = {
        notionPageId: response.id,
        morgenEventId: morgen.morgenEventId,
        title: morgen.title,
        description: morgen.description,
        date: morgen.date,
        duration: morgen.duration,
        lastUpdate: response.last_edited_time,
      };
      
      const newKey = generateEventPairKey(eventPair);
      newMap[newKey] = eventPair;

  } catch (error) {
      console.error('Error creating Notion task:', error);
  }
  }
}

/**
 * Creates a new event in Morgen from a Notion task.
 * This function converts a Notion task into a format suitable for Morgen and creates a new event.
 * It also updates the event mapping to associate the newly created Morgen event with the Notion task.
 *
 * @param {UniversalTaskEvent} task - The Notion task to be converted into a Morgen event.
 * @param {{ [key: string]: EventPair }} newMap - The map to update with the new Morgen event.
 */
async function createMorgenEventFromTask(task: UniversalTaskEvent, newMap: { [key: string]: EventPair }) {
  const API_URL = "https://api.morgen.so/v3/events/create";

  const idPlusDescription = task.notionPageId ? `#PAGEID:${task.notionPageId}#${task.description}`: task.description;

  const hasTime = task.date.includes('T');

  const requestBody = {
    accountId: process.env.MORGEN_ACCOUNT_ID,
    calendarId: process.env.MORGEN_CALENDAR_ID,
    title: task.title,
    description: idPlusDescription,
    start: formatToMorgenDateTime(task.date),
    duration: task.duration,
    showWithoutTime: !hasTime,
    timeZone: "UTC"
  };

  try {
    const response = await axios.post(API_URL, requestBody, {
      headers: {
        "accept": "application/json",
        "Authorization": `ApiKey ${process.env.MORGEN_API_KEY}`
      }
    });
    
    console.log('Morgen event created successfully:');

  if (task.notionPageId) {
    task.morgenEventId = response.data.data.event.id;
    await updateNotionPageWithMorgenEventId(task.notionPageId, task.morgenEventId);
    const eventPair: EventPair = {
      notion: task,
      morgen: {
        ...task, 
        morgenEventId: task.morgenEventId
      }
    };
    const newKey = generateEventPairKey(eventPair);
    newMap[newKey] = eventPair;
  }
    
  } catch (error) {
    console.error('Error in createMorgenEventFromTask:', error.response?.data || error.message);
  }

}

// Update ID Functions
/**
 * Updates a Notion page with the ID of a corresponding Morgen event.
 * This function is typically used to link a Notion task to its equivalent event in Morgen.
 *
 * @param {string} notionPageId - The ID of the Notion page to be updated.
 * @param {string} morgenEventId - The ID of the Morgen event to be linked.
 */
async function updateNotionPageWithMorgenEventId(notionPageId, morgenEventId) {
  try {
    const response = await notion.pages.update({
      page_id: notionPageId,
      properties: {
        'Morgen Event ID': {
          rich_text: [
            {
              text: {
                content: morgenEventId
              }
            }
          ]
        }
      }
    });
    console.log('Notion page updated with Morgen Event ID: ');
  } catch (error) {
    console.log('Error updating Notion page: error');
  }
}

/**
 * Updates a Morgen event with the ID of a corresponding Notion page.
 * This function is typically used to link a Morgen event to its equivalent task in Notion.
 *
 * @param {string} notionPageId - The ID of the Notion page to be linked.
 * @param {string} morgenEventId - The ID of the Morgen event to be updated.
 * @param {string} currentDescription - The current description of the Morgen event.
 */
async function updateMorgenEventWithNotionPageId(notionPageId, morgenEventId, currentDescription) {
  console.log("updating morgen event with notion page id...")

  const UPDATE_MODE = "single";
  const API_URL = "https://api.morgen.so/v3/events/update?seriesUpdateMode=" + UPDATE_MODE;

  // check if description already contains a PAGEID tag and update accordingly
  const notionIdPattern = /^#PAGEID:([\w-]+)#/;
  const existingMatch = notionIdPattern.exec(currentDescription);
  let newDescription = currentDescription;

  // if  PAGEID tag is not present in the description, add it to front
  if (!existingMatch) {
    newDescription = `#PAGEID:${notionPageId}# ${currentDescription}`;
  }

  const requestBody = {
    accountId: process.env.MORGEN_ACCOUNT_ID,
    calendarId: process.env.MORGEN_CALENDAR_ID,
    id: morgenEventId,
    description: newDescription,
  };

  try {
    const response = await axios.post(API_URL, requestBody, {
      headers: {
        "accept": "application/json",
        "Authorization": `ApiKey ${process.env.MORGEN_API_KEY}`
      }
    });
    console.log('Morgen event updated succesfully:')
  } catch (error) {
    console.log('Error updating Morgen event:', error)
  }

}

// Get Task/Event functions
/**
 * Fetches events from the Morgen API within a specified time range.
 * This function makes a request to the Morgen API and retrieves events between the 'from' and 'to' dates.
 *
 * @param {DateTime} from - The start date/time for fetching events.
 * @param {DateTime} to - The end date/time for fetching events.
 * @returns {Promise<MorgenEvent[]>} - A promise that resolves to an array of Morgen events.
 */
async function getEventsFromMorgenAPI(from:DateTime, to:DateTime): Promise<MorgenEvent[]> {
  const url = `https://api.morgen.so/v3/events/list?accountId=${process.env.MORGEN_ACCOUNT_ID}&calendarIds=${process.env.MORGEN_CALENDAR_ID}&start=${from.toISO({ suppressMilliseconds: true, includeOffset: false })}&end=${to.toISO({ suppressMilliseconds: true, includeOffset: false })}`;

  try {
    const response = await axios.get(url, {
      headers: {
        "accept": "application/json",
        "Authorization": `ApiKey ${process.env.MORGEN_API_KEY}`
      }
    });

    try {
      const morgenEvents = convertToMorgenEvents(response.data.data);
      return morgenEvents
    } catch (error) {
        console.error("Error converting events:", error.message);
    }

  } catch (error) {
    console.error('Error fetching events:', error.response?.data || error.message);
  }
  return [];
}

/**
 * Converts the raw data from the Morgen API to an array of MorgenEvent objects.
 * This function processes the raw event data, extracting relevant information and formatting it as needed.
 *
 * @param {any} data - The raw data from the Morgen API.
 * @returns {MorgenEvent[]} - An array of MorgenEvent objects.
 */
function convertToMorgenEvents(data:any): MorgenEvent[] {
  if (!data.events) {
    throw new Error('data.events is missing or undefined');
  }

  if (!Array.isArray(data.events)) {
    console.error('Unexpected: data.events is not an array', data.events);
  }

  return data.events.map(event => {
    let pageId = '';
    let description = event.description ?? '';

    // extract notion pageid and adjust description
    const notionIdPattern = /^#PAGEID:([\w-]+)#/;
    const match = notionIdPattern.exec(description);
    if (match) {
      pageId = match[1];
      description = description.replace(notionIdPattern,'');
    }

    const eventStartInUTC = DateTime.fromISO(event.start, { zone: event.timeZone }).toISO();

    return {
      eventId: event.id || '',
      title: event.title || '',
      description: description || '',
      start: eventStartInUTC || '',
      duration: event.duration || '',
      pageId: pageId || '',
      lastUpdate: event.updated || ''
    };
  })
}

/**
 * Fetches tasks from a Notion database within a specified time range.
 * This function queries the Notion database for tasks that fall between the 'from' and 'to' dates.
 *
 * @param {DateTime} from - The start date/time for fetching tasks.
 * @param {DateTime} to - The end date/time for fetching tasks.
 * @returns {Promise<Task[]>} - A promise that resolves to an array of Notion tasks.
 */
async function getTasksFromNotionDatabase(from:DateTime, to:DateTime): Promise<Task[]> {
  const pages = []
  let cursor = undefined

  const shouldContinue = true
  while (shouldContinue) {
    const { results, next_cursor } = await notion.databases.query({
      database_id: databaseId,
      filter: {
        "and": [
          {
            "property": "Due date",
            "date": {
              "on_or_after": formatToISODate(from)
            }
          },
          {
            "property": "Due date",
            "date": {
              "on_or_before": formatToISODate(to)
            }
          }
        ]
      },
      start_cursor: cursor,
    })
    pages.push(...results)
    if (!next_cursor) {
      break
    }
    cursor = next_cursor
  }
  console.log(`${pages.length} pages successfully fetched.`)

  const tasks:Task[] = []
  for (const page of pages) {

    if (!isValidTask(page)) {
      console.log(`Skipping incomplete task with page id:${page.id}`);
      continue;
    }

    tasks.push({
      pageId: page.id,
      title: getTitlePropertyValue(page.properties["Name"]),
      dueDate: getDueDatePropertyValue(page.properties["Due date"]),
      description: getDescriptionPropertyValue(page.properties["Description"]),
      area: getAreaPropertyValue(page.properties["Area"]),
      status: getStatusPropertyValue(page.properties["Status"]),
      eventId: getEventIdPropertyValue(page.properties["Morgen Event ID"]),
      lastUpdate: getLastUpdatePropertyValue(page.properties["Last Update"]),
    });
  }
  return tasks;
}

/**
 * Validates a Notion page to check if it represents a complete task.
 * It checks for the presence and correctness of required properties like Name, Due date, Description, Area, and Status.
 *
 * @param {any} page - The Notion page object to be validated.
 * @returns {boolean} - Returns true if the page has all required properties and they are correctly formatted, false otherwise.
 */
function isValidTask(page: any): boolean {
  const requiredProperties = ["Name", "Due date", "Description", "Area", "Status"];
  
  for (const propName of requiredProperties) {
    const property = page.properties[propName];
    if (!property) {
      return false;
    }
    
    switch(propName) {
      case "Name":
        if (property.type !== "title" || !property.title.length) return false;
        break;
      case "Due date":
        if (property.type !== "date" || !property.date || !property.date.start) return false;
        break;
      case "Description":
        if (property.type !== "rich_text" || !property.rich_text.length) return false;
        break;
      case "Area":
        if (property.type !== "select" || !property.select) return false;
        break;
      case "Status":
        if (property.type !== "status" || !property.status) return false;
        break;
      default:
        break;
    }
  }
  return true;
}

// Notion Get-Property Functions
/**
 * Extracts the status property value from a Notion page property.
 *
 * @param {PropertyItemObjectResponse | Array<PropertyItemObjectResponse>} property - The Notion property object.
 * @returns {string} - Returns the status name if available, or "No status" if not.
 */
function getStatusPropertyValue( property: PropertyItemObjectResponse | Array<PropertyItemObjectResponse> ): string {
  if (Array.isArray(property)) {
    property = property[0];
  }
  return property.type === "status" ? property.status.name : "No status";
}

/**
 * Extracts the title property value from a Notion page property.
 *
 * @param {any} property - The Notion title property object.
 * @returns {string} - Returns the title as a string, or "No Title" if no title is present.
 */
function getTitlePropertyValue(property: any): string {
  if (property?.type === "title") {
    return property.title.map((textObj: any) => textObj.plain_text).join('');
  }
  return "No Title";
}

/**
 * Extracts the event ID property value from a Notion page property.
 *
 * @param {any} property - The Notion rich_text property object.
 * @returns {string} - Returns the event ID as a string, or an empty string if no ID is present.
 */
function getEventIdPropertyValue(property: any): string {
  if (property?.type === "rich_text") {
    return property.rich_text.map((textObj: any) => textObj.plain_text).join('').trim();
  }
  return "";
}

/**
 * Extracts the due date property value from a Notion page property.
 *
 * @param {any} property - The Notion date property object.
 * @returns {string} - Returns the due date in ISO format, or "No Due date" if no date is present.
 */
function getDueDatePropertyValue(property: any): string {
  if (property?.type === "date") {
    const hasTime = property.date.start.includes('T');

    if (!hasTime) {
      return property.date.start;
    }

    // if there is a time component, convert to UTC and format
    const utcDateStart = DateTime.fromISO(property.date.start).toUTC().toISO() || 'date conversion failed';
    return utcDateStart;
  }
  return "No Due date";
}

/**
 * Extracts the description property value from a Notion page property.
 *
 * @param {any} property - The Notion rich_text property object.
 * @returns {string} - Returns the description as a string, or "No Description" if no description is present.
 */
function getDescriptionPropertyValue(property: any): string {
  if (property?.type === "rich_text") {
    return property.rich_text.map((textObj: any) => textObj.plain_text).join('');
  }
  return "No Description";
}

/**
 * Extracts the area property value from a Notion page property.
 *
 * @param {any} property - The Notion select property object.
 * @returns {string} - Returns the area name, or "No Area" if no area is defined.
 */
function getAreaPropertyValue(property): string {
  if (property.type === "select") {
    return property.select.name;
  } else {
    return "No Area";
  }
}

/**
 * Extracts the last update property value from a Notion page property.
 *
 * @param {any} property - The Notion property object containing last edit time.
 * @returns {string} - Returns the last update time in ISO format, or "No last edited time" if not available.
 */

function getLastUpdatePropertyValue(property): string {
  if (property?.type === "last_edited_time") {
    return property.last_edited_time
  }
  return "No last edited time"
}

// Generate Key Functions
/**
 * Generates a unique key for an event pair based on Morgen and Notion event IDs.
 *
 * @param {EventPair} eventPair - The event pair object containing Morgen and/or Notion event information.
 * @returns {string} - A unique key string for the event pair.
 */
function generateEventPairKey(eventPair:EventPair):string {
  if (eventPair.morgen) {
    return `${eventPair.morgen.morgenEventId || ''}|${eventPair.morgen.notionPageId || ''}`;
  } else if (eventPair.notion) {
    return `${eventPair.notion.morgenEventId || ''}|${eventPair.notion.notionPageId || ''}`;
  }
  return "morgen and notion did not exist in event pair!"
}

/**
 * Generates a unique key for an event based on its Morgen and Notion IDs.
 *
 * @param {UniversalTaskEvent} event - The event object containing Morgen and/or Notion event information.
 * @returns {string} - A unique key string for the event.
 */
function generateEventKey(event:UniversalTaskEvent):string {
  return `${event.morgenEventId || ''}|${event.notionPageId || ''}`;
}

// Formatting Date Functions
/**
 * Formats a date-time string to the Morgen API's expected date-time format.
 *
 * @param {string} dateTime - The date-time string to format.
 * @param {string} [zone='utc'] - The timezone to use for formatting.
 * @returns {string} - The formatted date-time string.
 */
function formatToMorgenDateTime(dateTime, zone = 'utc') {
  return DateTime.fromISO(dateTime, { zone })
    .toUTC()
    .toFormat("yyyy-MM-dd'T'HH:mm:ss");
}

/**
 * Formats a DateTime object to an ISO date string.
 *
 * @param {DateTime} dateTime - The DateTime object to format.
 * @returns {string} - The ISO date string.
 */
function formatToISODate(dateTime) {
  return dateTime.toISO();
}
